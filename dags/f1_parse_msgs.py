from tg_jobs_parser.google_cloud_helper.storage_manager import StorageManager
from tg_jobs_parser.telegram_helper.message_parser import MessageParser
from tg_jobs_parser.utils import json_helper
from tg_jobs_parser.configs import vars, volume_folder_path
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import os
import logging

# metadata downloaded from cloud
CL_CHANNELS_LOCAL_PATH = os.path.join(volume_folder_path, 'gsc_channels_metadata.json')
# metadata parser from telegram
TG_CHANNELS_LOCAL_PATH = os.path.join(volume_folder_path, 'tg_channels_metadata.json')
# metadata merged locally
MG_CHANNELS_LOCAL_PATH = os.path.join(volume_folder_path, 'merged_channels_metadata.json')
# metadata uploadede to cloud (messages)
UP_CHANNELS_LOCAL_PATH = os.path.join(volume_folder_path, 'uploaded_msgs_metadata.json')


class Context:
    def __init__(self):
        self.storage_manager = StorageManager()
        self.msg_parser = MessageParser()


ctx = Context()


def get_chunk_msgs(parser, channel, save_to=volume_folder_path):
    """
    job two - read messages from channel
    # save in to json & avro files locally
    :param parser:
    :param save_to:
    :param channel:
    :return: just save parsed msges to volume path
    """
    # flow right: target_id or right border =>>>  last_posted_message_id.
    # flow left:  target_id <<<= left border or last_posted_message_id
    print(f'{channel} will be saved to {save_to}')
    msgs, left, right = parser.run_chat_parser(channel)
    if msgs:
        json_helper.save_to_line_delimited_json(
            msgs,
            os.path.join(save_to, f"msgs{channel['id']}_left_{left}_right_{right}.json")
        )
    else:
        print(f"Nothing to save channel: {channel['id']}")


@task
def parse_messages():
    """
    get channels_metadata from GCS
    each channel run job  two 'get messages' (saved to_json)
    :return:
    """
    channels = json_helper.read_json(CL_CHANNELS_LOCAL_PATH)
    for ch_id in channels:
        if channels[ch_id]['status'] == 'bad' or channels[ch_id]['type'] != 'ChatType.CHANNEL':
            continue
        get_chunk_msgs(ctx.msg_parser, channels[ch_id])


@task
def check_unparsed_msgs():
    """
    just check how much messages we have
    :return:
    """
    channels_total = 0
    channels_done = 0
    channels_to_update = 0
    bad_channels = 0
    total_difference = 0
    bad_channels_ids = []
    to_upd_channels_ids = []

    ctx.storage_manager.download_channels_metadata(path=CL_CHANNELS_LOCAL_PATH)
    channels = json_helper.read_json(CL_CHANNELS_LOCAL_PATH)

    for group in channels.values():
        if group["status"] == "ok" and group['type'] == 'ChatType.CHANNEL' \
                and "last_posted_message_id" in group and "target_id" in group:
            difference = group["last_posted_message_id"] - group["target_id"] - (
                    (group.get("right_saved_id") or 0) - (group.get("left_saved_id") or 0))
            total_difference += difference
            channels_total += 1

            if difference == 0:
                channels_done += 1
            elif difference > 0:
                channels_to_update += 1
                to_upd_channels_ids.append(group.get("id"))
            else:
                bad_channels += 1
                bad_channels_ids.append(group.get("id"))

    logging.info(f'need to download total: {total_difference}')
    logging.info(f'channels_total: {channels_total}')
    logging.info(f'channels_done: {channels_done}')
    logging.info(f'channels_to_update: {channels_to_update}')
    logging.info(f'channels_to_update_ids: {to_upd_channels_ids}')
    logging.info(f'bad_channels: {bad_channels}')
    logging.info(f'bad_channels_ids: {bad_channels_ids}')


@task
def upload_msgs_to_cloud():
    """
    after all messages downloaded  load to GCS
    update cloud metadata with left and right ids if loaded OK
    :return:
    """
    # storage_manager.download_channels_metadata(path=CL_CHANNELS_LOCAL_PATH)
    results = {}
    for filename in os.listdir(volume_folder_path):
        match = vars.MSGS_FILE_PATTERN.match(filename)
        if match:
            chat_id = match.group('chat_id')
            left = int(match.group('left'))
            right = int(match.group('right'))
            blob_path = f'{chat_id}/{filename}'
            uploaded = ctx.storage_manager.upload_message(os.path.join(volume_folder_path, filename), blob_path)
            # uploaded = True
            if uploaded:
                results[chat_id] = {
                    'new_left_saved_id': left,
                    'new_right_saved_id': right,
                    'uploaded_path': blob_path
                }
                ctx.storage_manager.delete_local_file(os.path.join(volume_folder_path, filename))
    json_helper.save_to_json(results, UP_CHANNELS_LOCAL_PATH)
    json_helper.update_uploaded_borders(CL_CHANNELS_LOCAL_PATH, UP_CHANNELS_LOCAL_PATH, MG_CHANNELS_LOCAL_PATH)
    ctx.storage_manager.update_channels_metadata(MG_CHANNELS_LOCAL_PATH)


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
        'parse_messages',
        default_args=default_args,
        schedule_interval='15 */1 * * *',
        catchup=False,
        max_active_runs=1,
        tags=['f1', 'message-processing', 'gcs', 'tg_jobs_parser']

) as dag:
    stats_task = check_unparsed_msgs()
    parse_msgs_task = parse_messages()
    upload_task = upload_msgs_to_cloud()

    stats_task >> parse_msgs_task >> upload_task
