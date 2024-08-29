from datetime import datetime
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from tg_jobs_parser.google_cloud_helper.storage_manager import StorageManager
from tg_jobs_parser.telegram_helper.telegram_parser import TelegramParser
from tg_jobs_parser.utils import json_helper
from tg_jobs_parser.configs import vars, volume_folder_path

# Define paths
CL_CHANNELS_LOCAL_PATH = os.path.join(
    volume_folder_path, "f1.2_gsc_channels_metadata.json"
)
TG_CHANNELS_LOCAL_PATH = os.path.join(
    volume_folder_path, "f1.2_tg_channels_metadata.json"
)
MG_CHANNELS_LOCAL_PATH = os.path.join(
    volume_folder_path, "f1.2_merged_channels_metadata.json"
)
UP_CHANNELS_LOCAL_PATH = os.path.join(
    volume_folder_path, "f1.2_uploaded_msgs_metadata.json"
)

# Initialize necessary components
logging.info("Initializing StorageManager and MessageParser")
storage_manager = StorageManager()
msg_parser = TelegramParser()


def check_unparsed_msgs():
    try:
        logging.info("Checking stats")
        storage_manager.check_channel_stats()
        return True
    except Exception as e:
        raise Exception(f"Error Checking stats: {e}")


def parse_messages():
    try:
        logging.info("Downloading channels metadata")
        storage_manager.download_channels_metadata(path=CL_CHANNELS_LOCAL_PATH)
        channels = json_helper.read_json(CL_CHANNELS_LOCAL_PATH)
        process_status = 0
        process_len = len(channels.items())

        for ch_id, channel in channels.items():
            process_status += 1
            logging.info(f"processed {process_status} from {process_len} channels")
            if channel["status"] == "bad" or channel["type"] != "ChatType.CHANNEL":
                continue
            logging.info(f"Parsing messages for channel {ch_id}")
            msgs, left, right = msg_parser.run_chat_parser(channel)
            if msgs:
                json_helper.save_to_line_delimited_json(
                    msgs,
                    os.path.join(
                        volume_folder_path,
                        f"msgs{ch_id}_left_{left}_right_{right}.json",
                    ),
                )
            else:
                logging.info(f"Nothing to save for channel: {ch_id}")
        return True
    except Exception as e:
        raise Exception(f"Error parsing messages: {e}")


def upload_msgs_files_to_storage():
    try:
        logging.info("Uploading message files to storage")
        results = {}
        for filename in os.listdir(volume_folder_path):
            match = vars.MSGS_FILE_PATTERN.match(filename)
            if match:
                chat_id = match.group("chat_id")
                left = int(match.group("left"))
                right = int(match.group("right"))
                blob_path = f"{chat_id}/{filename}"
                uploaded = storage_manager.upload_message(
                    os.path.join(volume_folder_path, filename), blob_path
                )
                if uploaded:
                    results[chat_id] = {
                        "new_left_saved_id": left,
                        "new_right_saved_id": right,
                        "uploaded_path": blob_path,
                    }
                    storage_manager.delete_local_file(
                        os.path.join(volume_folder_path, filename)
                    )
        # Save info about uploaded files to storage
        json_helper.save_to_json(results, UP_CHANNELS_LOCAL_PATH)
        # Merge borders (cloud saved + just uploaded)
        json_helper.update_uploaded_borders(
            CL_CHANNELS_LOCAL_PATH, UP_CHANNELS_LOCAL_PATH, MG_CHANNELS_LOCAL_PATH
        )
        # Upload channels metadata with fresh borders
        storage_manager.update_channels_metadata(MG_CHANNELS_LOCAL_PATH)
        logging.info("Uploaded message files to storage successfully")
        return True
    except Exception as e:
        raise Exception(f"Error uploading messages to storage: {e}")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 6, 1),
}

with DAG(
    "f1_parse_messages",
    default_args=default_args,
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["f1", "step2", "message-processing", "gcs", "tg_jobs_parser"],
) as dag:
    stats_task = PythonOperator(
        task_id="t1_check_unparsed_msgs", python_callable=check_unparsed_msgs
    )

    parse_msgs_task = PythonOperator(
        task_id="t2_parse_messages", python_callable=parse_messages
    )

    upload_files_to_gcs_task = PythonOperator(
        task_id="t3_upload_files_to_storage",
        python_callable=upload_msgs_files_to_storage,
    )

    stats_task >> parse_msgs_task >> upload_files_to_gcs_task

# if __name__ == '__main__':
#     dag.test()
