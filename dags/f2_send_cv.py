from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import time
from tg_jobs_parser.telegram_helper.telegram_parser import TelegramParser
from tg_jobs_parser.google_cloud_helper.bigquery_manager import BigQueryManager
from tg_jobs_parser.utils import json_helper
import os

max_messages_limit = 2
day_limit = 7
TMP_FILE = os.path.join('/home/wudmc/PycharmProjects/jobs_parser/volume', "group_msg_statuses.json")
TABLE_ID = 'tg_jobs.group_msg_statuses'


def find_channels_by_name(channels, name):
    # [(-1001850188153,
    #   {'id': -1001850188153,
    #    'title': 'Dummy',
    #    'username': None,
    #    'members': 1,
    #    'type': 'ChatType.SUPERGROUP',
    #    'url': '',
    #    'last_posted_message_id': 26,
    #    'last_posted_message_data': '2024-09-06 16:33:47',
    #    'updated_at': '2024-09-06 16:36:09.592685'}),
    found_channels = []
    for channel_id, channel_info in channels.items():
        if channel_info.get('title') == name:
            found_channels.append((channel_id, channel_info))

    return found_channels if found_channels else None


def get_channels(**kwargs):
    """Получает каналы из Telegram и передает их через XCom."""
    logging.info("Downloading channels metadata")
    msg_parser = TelegramParser()
    channels = msg_parser.get_channels()
    kwargs['ti'].xcom_push(key='channels', value=channels)


def get_statuses(**kwargs):
    """Получает статусы сообщений из BigQuery и передает их через XCom."""
    logging.info("Fetching message statuses from BigQuery")
    new_datetime = datetime.now() + timedelta(days=day_limit)
    bigquery_manager = BigQueryManager("geo-roulette")
    channels_statuses = bigquery_manager.select_all(dataset_id='tg_jobs', table_id='group_msg_statuses')
    kwargs['ti'].xcom_push(key='statuses', value=channels_statuses)
    kwargs['ti'].xcom_push(key='new_datetime', value=new_datetime.isoformat())


def send_messages(**kwargs):
    """Отправляет сообщения на каналы, используя данные из XCom."""
    channels = kwargs['ti'].xcom_pull(key='channels', task_ids='get_channels')
    channels_statuses = kwargs['ti'].xcom_pull(key='statuses', task_ids='get_statuses')
    new_datetime = datetime.fromisoformat(kwargs['ti'].xcom_pull(key='new_datetime', task_ids='get_statuses'))
    bigquery_manager = BigQueryManager("geo-roulette")
    msg_parser = TelegramParser()

    msgs_sent = 0
    results = []

    for ch_id, channel in channels.items():
        if channel["type"] != "ChatType.SUPERGROUP" and channel["type"] != "ChatType.GROUP":
            continue
        last_result = [item for item in channels_statuses if item['chat_id'] == str(ch_id)]
        if last_result and last_result[-1]['datetime'] < new_datetime:
            logging.info(
                f"Message was sent less than {day_limit} days ago to channel {ch_id}: {channel['title']}, {channel['url']}, at {last_result[-1]['datetime']}")
            continue

        logging.info(f"Sending message to channel {ch_id}: {channel['title']}, {channel['url']}")
        try:
            response = msg_parser.send_cv('wudmc')
            # response = msg_parser.send_cv(int(ch_id))
            result = {'result': True, 'chat_id': channel['title'], 'msg_id': response.link,
                      'datetime': str(response.date), 'error': None}
        except Exception as e:
            result = {'result': False, 'chat_id': channel['title'], 'msg_id': None, 'datetime': str(datetime.now()),
                      'error': str(e)}
        msgs_sent += 1
        results.append(result)
        logging.info(f"messages sent: {msgs_sent} from {max_messages_limit} max limit")
        time.sleep(30)

        if msgs_sent >= max_messages_limit:
            break
    if results:
        json_helper.save_to_line_delimited_json(results, TMP_FILE)
        bigquery_manager.load_json_to_bigquery(TMP_FILE, TABLE_ID)
    else:
        logging.info('Nothing to send to BigQuery')


# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'send_telegram_messages_split',
        default_args=default_args,
        description='A DAG to send Telegram messages with parallel tasks and XComs',
        schedule="0 9-17/2 * * 1-5",
        catchup=False,
        max_active_runs=1,
        tags=["f2"],

) as dag:
    get_channels_task = PythonOperator(
        task_id='get_channels',
        python_callable=get_channels,
    )

    get_statuses_task = PythonOperator(
        task_id='get_statuses',
        python_callable=get_statuses,
    )

    send_messages_task = PythonOperator(
        task_id='send_messages',
        python_callable=send_messages,
    )

    # Определяем порядок выполнения задач
    [get_channels_task, get_statuses_task] >> send_messages_task

if __name__ == "__main__":
    dag.test()
