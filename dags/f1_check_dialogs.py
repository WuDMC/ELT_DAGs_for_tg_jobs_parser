from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import logging

from tg_jobs_parser.telegram_helper.telegram_parser import TelegramParser
from tg_jobs_parser.google_cloud_helper.storage_manager import StorageManager
from tg_jobs_parser.utils import json_helper
from tg_jobs_parser.configs import vars, volume_folder_path

# Define tmp paths
CL_CHANNELS_LOCAL_PATH = os.path.join(
    volume_folder_path, "f1.1_gsc_channels_metadata.json"
)
TG_CHANNELS_LOCAL_PATH = os.path.join(
    volume_folder_path, "f1.1_tg_channels_metadata.json"
)
MG_CHANNELS_LOCAL_PATH = os.path.join(
    volume_folder_path, "f1.1_merged_channels_metadata.json"
)


def parse_tg_dialogs(tg_parser):
    try:
        logging.info("Start parsing tg dialogs")
        json_helper.save_to_json(
            data=tg_parser.get_channels(), path=TG_CHANNELS_LOCAL_PATH
        )
        logging.info(f"Saved dialogs to {TG_CHANNELS_LOCAL_PATH}")
        return True
    except Exception as e:
        raise Exception(f"Error Getting metadata from cloud: {e}")


def get_metadata_from_cloud(storage_manager):
    try:
        logging.info("Getting metadata from cloud")
        storage_manager.download_channels_metadata(path=CL_CHANNELS_LOCAL_PATH)
        return True
    except Exception as e:
        raise Exception(f"Error Getting metadata from cloud: {e}")


def update_target_ids(tg_parser, date=vars.START_DATE, force=False):
    try:
        logging.info(
            f"Updating metadata locally. Attrs ( date: {date}, force: {force})"
        )
        cloud_channels = json_helper.read_json(CL_CHANNELS_LOCAL_PATH) or {}
        tg_channels = json_helper.read_json(TG_CHANNELS_LOCAL_PATH)
        # set to cloud_channels target id from parsed dialogs
        json_helper.set_target_ids(tg_channels, cloud_channels, tg_parser, date, force)
        # save updated cloud_channels
        json_helper.save_to_json(cloud_channels, CL_CHANNELS_LOCAL_PATH)
        logging.info(f"Saved updated metadata to {CL_CHANNELS_LOCAL_PATH}")
        return True
    except Exception as e:
        raise Exception(f"Error Updating metadata locally: {e}")


def update_last_updated_ids():
    try:
        logging.info("Merging metadata to update last posted ids in tg channels")
        json_helper.merge_json_files(
            file1_path=CL_CHANNELS_LOCAL_PATH,
            file2_path=TG_CHANNELS_LOCAL_PATH,
            output_path=MG_CHANNELS_LOCAL_PATH,
        )
        logging.info(f"Merged metadata saved to {MG_CHANNELS_LOCAL_PATH}")
        return True
    except Exception as e:
        raise Exception(f"Error merging metadata locally: {e}")


def update_metadata_in_cloud(storage_manager):
    try:
        logging.info("Updating metadata in cloud")
        storage_manager.update_channels_metadata(
            source_file_name=MG_CHANNELS_LOCAL_PATH
        )
        logging.info(
            f"Updated cloud metadata with {MG_CHANNELS_LOCAL_PATH}. Now you can use it"
        )
        return True
    except Exception as e:
        raise Exception(f"Error Updating metadata in cloud: {e}")


def delete_tmp_files():
    try:
        os.remove(CL_CHANNELS_LOCAL_PATH)
        os.remove(TG_CHANNELS_LOCAL_PATH)
        os.remove(MG_CHANNELS_LOCAL_PATH)
        logging.info("Files deleted successfully.")
    except FileNotFoundError:
        logging.info("One or more files were not found.")
    except Exception as e:
        logging.info(f"An error occurred: {e}")


def check_unparsed_msgs(storage_manager):
    try:
        logging.info("Checking stats")
        storage_manager.check_channel_stats()
        return True
    except Exception as e:
        raise Exception(f"Error Checking stats: {e}")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 6, 1),
}

with DAG(
    "f1_check_dialogs",
    default_args=default_args,
    schedule="0 */3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["f1","step1", "message-processing", "gcs", "tg_jobs_parser"],
) as dag:
    # Initialize components once and pass them as arguments
    logging.info("top level code")
    tg_parser_instance = TelegramParser()
    storage_manager_instance = StorageManager()

    parse_tg_dialogs_task = PythonOperator(
        task_id="t1_parse_tg_dialogs",
        python_callable=parse_tg_dialogs,
        op_kwargs={"tg_parser": tg_parser_instance},
    )

    get_cloud_metadata_task = PythonOperator(
        task_id="t2_get_metadata_from_cloud",
        python_callable=get_metadata_from_cloud,
        op_kwargs={"storage_manager": storage_manager_instance},
    )

    update_target_ids_task = PythonOperator(
        task_id="t3_update_target_ids",
        python_callable=update_target_ids,
        op_kwargs={"tg_parser": tg_parser_instance},
    )

    update_last_updated_ids_task = PythonOperator(
        task_id="t4_update_last_updated_ids", python_callable=update_last_updated_ids
    )

    update_cloud_metadata_task = PythonOperator(
        task_id="t5_update_metadata_in_cloud",
        python_callable=update_metadata_in_cloud,
        op_kwargs={"storage_manager": storage_manager_instance},
    )

    delete_tmp_files_task = PythonOperator(
        task_id="t6_delete_tmp_files",
        python_callable=delete_tmp_files,
    )

    stats_task = PythonOperator(
        task_id="t7_check_unparsed_msgs",
        python_callable=check_unparsed_msgs,
        op_kwargs={"storage_manager": storage_manager_instance},
    )

    (
        parse_tg_dialogs_task
        >> get_cloud_metadata_task
        >> update_target_ids_task
        >> update_last_updated_ids_task
        >> update_cloud_metadata_task
        >> delete_tmp_files_task
        >> stats_task
    )

if __name__ == "__main__":
    dag.test()
