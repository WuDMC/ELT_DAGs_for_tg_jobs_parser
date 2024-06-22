from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import os
import logging

from tg_jobs_parser.telegram_helper.channel_parser import ChannelParser
from tg_jobs_parser.google_cloud_helper.storage_manager import StorageManager
from tg_jobs_parser.utils import json_helper
from tg_jobs_parser.configs import vars, volume_folder_path
from tg_jobs_parser.telegram_helper.message_parser import MessageParser


# Путь к локальным файлам метаданных
CL_CHANNELS_LOCAL_PATH = os.path.join(volume_folder_path, 'gsc_channels_metadata.json')
TG_CHANNELS_LOCAL_PATH = os.path.join(volume_folder_path, 'tg_channels_metadata.json')
MG_CHANNELS_LOCAL_PATH = os.path.join(volume_folder_path, 'merged_channels_metadata.json')


class Context:
    def __init__(self):
        self.storage_manager = StorageManager()
        self.channel_parser = ChannelParser()
        self.msg_parser = MessageParser()


ctx = Context()

# Определяем задачи DAG
@task
def init_channel_parser_and_parse_tg_dialog():
    logging.info('Initializing channel parser and parsing Telegram dialog')
    dialogs = ctx.channel_parser.get_channels()
    json_helper.save_to_json(data=dialogs, path=TG_CHANNELS_LOCAL_PATH)
    logging.info(f'Saved dialogs to {TG_CHANNELS_LOCAL_PATH}')
    return TG_CHANNELS_LOCAL_PATH

@task
def get_metadata_from_cloud():
    logging.info('Getting metadata from cloud')
    ctx.storage_manager.download_channels_metadata(path=CL_CHANNELS_LOCAL_PATH)
    logging.info(f'Downloaded metadata to {CL_CHANNELS_LOCAL_PATH}')
    return CL_CHANNELS_LOCAL_PATH

@task
def update_metadata_locally(date=vars.START_DATE, force=False):
    logging.info(f'Updating metadata locally. Attrs ( date: {date}, force: {force})')
    cloud_channels = json_helper.read_json(CL_CHANNELS_LOCAL_PATH) or {}
    tg_channels = json_helper.read_json(TG_CHANNELS_LOCAL_PATH)
    try:
        json_helper.refresh_status(tg_channels, cloud_channels, ctx.msg_parser, date, force)
    finally:
        json_helper.save_to_json(cloud_channels, CL_CHANNELS_LOCAL_PATH)
        logging.info(f'Saved updated metadata to {CL_CHANNELS_LOCAL_PATH}')
        return CL_CHANNELS_LOCAL_PATH


@task
def merge_metadata():
    logging.info('Merging metadata')
    json_helper.merge_json_files(file1_path=CL_CHANNELS_LOCAL_PATH, file2_path=TG_CHANNELS_LOCAL_PATH,
                                 output_path=MG_CHANNELS_LOCAL_PATH)
    logging.info(f'Merged metadata saved to {MG_CHANNELS_LOCAL_PATH}')
    return MG_CHANNELS_LOCAL_PATH

@task
def update_metadata_in_cloud():
    logging.info('Updating metadata in cloud')
    ctx.storage_manager.update_channels_metadata(source_file_name=MG_CHANNELS_LOCAL_PATH)
    logging.info(f'Updated cloud metadata with {MG_CHANNELS_LOCAL_PATH}')

@task
def check_unparsed_msgs():
    """
    just check how much messages we have
    :return:
    """
    ctx.storage_manager.download_channels_metadata(path=CL_CHANNELS_LOCAL_PATH)
    channels = json_helper.read_json(CL_CHANNELS_LOCAL_PATH)
    json_helper.calc_stats(channels)


# Определяем параметры по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 6, 1),
}
# Определяем DAG
with DAG(
    'f1_check_dialogs',
    default_args=default_args,
    schedule_interval='0 */3 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['f1', 'message-processing', 'gcs', 'tg_jobs_parser']
) as dag:
    # Определяем порядок выполнения задач
    tg_dialog_task = init_channel_parser_and_parse_tg_dialog()
    cloud_metadata_task = get_metadata_from_cloud()
    update_local_metadata_task = update_metadata_locally()
    merge_metadata_task = merge_metadata()
    update_cloud_metadata_task = update_metadata_in_cloud()
    stats_task = check_unparsed_msgs()

    # Задаем зависимости между задачами
    tg_dialog_task >> cloud_metadata_task >> update_local_metadata_task >> merge_metadata_task >> update_cloud_metadata_task >> stats_task
