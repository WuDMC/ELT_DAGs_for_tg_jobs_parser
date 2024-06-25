import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from google.api_core.exceptions import NotFound

from tg_jobs_parser.google_cloud_helper.storage_manager import StorageManager
from tg_jobs_parser.google_cloud_helper.bigquery_manager import BigQueryManager
from tg_jobs_parser.configs import vars, volume_folder_path
from tg_jobs_parser.utils import json_helper

# Define paths
TMP_FILE = os.path.join(volume_folder_path, 'tmp_not_processed_msg_files.json')

# Initialize necessary components
logging.info('Initializing StorageManager and BigQueryManager')
storage_manager = StorageManager()
bigquery_manager = BigQueryManager(vars.PROJECT_ID)


def list_msgs():
    try:
        logging.info("Listing messages from storage.")
        return storage_manager.list_msgs_with_metadata()
    except Exception as e:
        raise Exception(f'Error listing messages from storage: {e}')


def check_table_exists():
    try:
        logging.info(f"Checking if table {vars.BIGQUERY_DATASET}.{vars.BIGQUERY_UPLOAD_STATUS_TABLE} exists.")
        bigquery_manager.client.get_table(f"{vars.BIGQUERY_DATASET}.{vars.BIGQUERY_UPLOAD_STATUS_TABLE}")
        return True
    except NotFound:
        logging.warning("Table not found.")
        return False


def check_msg_files_in_process():
    if check_table_exists():
        try:
            logging.info("Fetching uploaded files from BigQuery.")
            return bigquery_manager.fetch_data(
                dataset_id=vars.BIGQUERY_DATASET,
                table_id=vars.BIGQUERY_UPLOAD_STATUS_TABLE,
                selected_fields='filename'
            )
        except Exception as e:
            raise Exception(f'Error fetching uploaded files from BigQuery: {e}')
    else:
        logging.warning("No uploaded files to fetch.")
        return []


def download_and_process_files(**kwargs):
    try:
        gsc_files = kwargs['ti'].xcom_pull(task_ids='t1_list_files')
        bigquery_files = kwargs['ti'].xcom_pull(task_ids='t2_check_msg_files_in_process')
        if not gsc_files:
            logging.info("No files found in storage.")
            return False

        existing_files_set = set(file['filename'] for file in bigquery_files)
        data = []
        process_status = 0
        process_len = len(gsc_files)
        for file in gsc_files:
            process_status += 1
            logging.info(f'processed {process_status} from {process_len} gsc_files')
            if file['name'] in existing_files_set:
                # logging.info(f"File {file['name']} already exists in BigQuery. Skipping.")
                continue

            match = vars.MSGS_FILE_PATTERN.match(file['name'])
            if not match:
                logging.warning(f"Filename {file['name']} does not match the expected pattern.")
                continue

            row = json_helper.make_row_msg_status(file, match, status='in process')
            data.append(row)

        if not data:
            logging.info("No new files to process.")
            return False

        # Save data to a temporary file
        json_helper.save_to_line_delimited_json(data, TMP_FILE)
        logging.info(f"Temporary file with msg_files statuses created at {TMP_FILE}")

        return TMP_FILE
    except Exception as e:
        raise Exception(f'Error downloading and processing files: {e}')


def update_status_table(file_path):
    try:
        logging.info(f"Uploading file {file_path} to BigQuery.")
        return bigquery_manager.load_json_to_bigquery(
            json_file_path=file_path,
            table_id=f'{vars.BIGQUERY_DATASET}.{vars.BIGQUERY_UPLOAD_STATUS_TABLE}'
        )
    except Exception as e:
        raise Exception(f'Error uploading file to BigQuery: {e}')


def clear_tmp_files(file_path):
    try:
        os.remove(file_path)
        logging.info(f'Temporary file {file_path} deleted')
    except OSError as e:
        logging.error(f'Error deleting temporary file {file_path}: {e}')


def check_stats():
    try:
        logging.info(f' checking {vars.BIGQUERY_DATASET}.{vars.BIGQUERY_UPLOAD_STATUS_TABLE}')
        bigquery_manager.check_table_stats(f'{vars.BIGQUERY_DATASET}.{vars.BIGQUERY_UPLOAD_STATUS_TABLE}')
        logging.info(' checking channels metadata json')
        storage_manager.check_channel_stats()
        logging.info(f' checking {vars.BIGQUERY_DATASET}.{vars.BIGQUERY_RAW_MESSAGES_TABLE}')
        bigquery_manager.check_table_stats(f'{vars.BIGQUERY_DATASET}.{vars.BIGQUERY_RAW_MESSAGES_TABLE}')
        return True
    except Exception as e:
        raise Exception(f'Error Checking stats: {e}')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 1),
}

with DAG(
        'f1_msgs_to_process',
        default_args=default_args,
        schedule='15 */6 * * *',
        catchup=False,
        max_active_runs=1,
        tags=['f1', 'message-processing', 'bigquery', 'gcs', 'tg_jobs_parser']
) as dag:
    list_raw_msgs_files = PythonOperator(
        task_id='t1_list_files',
        python_callable=list_msgs,
    )

    check_msg_files_in_process = PythonOperator(
        task_id='t2_check_msg_files_in_process',
        python_callable=check_msg_files_in_process,
    )

    process_new_files = ShortCircuitOperator(
        task_id='t3_get_files_to_process_task',
        python_callable=download_and_process_files,
        provide_context=True,
    )

    update_status_table = PythonOperator(
        task_id='t4_update_status_table',
        python_callable=update_status_table,
        op_kwargs={'file_path': "{{ ti.xcom_pull(task_ids='t3_get_files_to_process_task') }}"}
    )

    clear_tmp_files = PythonOperator(
        task_id='t5_clear_tmp_files_task',
        python_callable=clear_tmp_files,
        op_kwargs={'file_path': "{{ ti.xcom_pull(task_ids='t3_get_files_to_process_task') }}"}
    )

    trigger_f1_load_msg_to_bq = TriggerDagRunOperator(
        task_id='trigger_f1_load_msg_to_bq',
        trigger_dag_id='f1_load_msg_to_bq',
        wait_for_completion=False,
        logical_date='{{ execution_date }}'
    )

    stats_task = PythonOperator(
        task_id='t6_check_stats',
        python_callable=check_stats
    )

    list_raw_msgs_files >> check_msg_files_in_process >> process_new_files
    process_new_files >> update_status_table >> clear_tmp_files >> trigger_f1_load_msg_to_bq >> stats_task

if __name__ == '__main__':
    dag.test()
