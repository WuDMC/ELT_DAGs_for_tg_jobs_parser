import logging
import os
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from google.api_core.exceptions import NotFound
from tg_jobs_parser.google_cloud_helper.storage_manager import StorageManager
from tg_jobs_parser.google_cloud_helper.bigquery_manager import BigQueryManager
from tg_jobs_parser.configs import vars, volume_folder_path
from tg_jobs_parser.utils import json_helper

# Define paths
TMP_FILE = os.path.join(volume_folder_path, 'tmp_not_uploaded_msg_files.json')

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


def check_files_statuses():
    try:
        if check_table_exists():
            logging.info("Querying message files statuses from BigQuery.")
            return bigquery_manager.query_msg_files_statuses(
                dataset_id=vars.BIGQUERY_DATASET,
                table_id=vars.BIGQUERY_UPLOAD_STATUS_TABLE,
            )
        else:
            logging.warning("No uploaded files status to fetch.")
            return []
    except Exception as e:
        raise Exception(f'Error querying message files statuses: {e}')


def upload_msg_bq(path):
    try:
        if not path:
            logging.info('No files to process. Be happy!')
            return

        logging.info(f"Uploading file {path} to BigQuery.")
        return bigquery_manager.load_json_uri_to_bigquery(
            path=path,
            table_id=f'{vars.BIGQUERY_DATASET}.{vars.BIGQUERY_RAW_MESSAGES_TABLE}'
        )
    except Exception as e:
        raise Exception(f'Error uploading file to BigQuery: {e}')


def process_all_files(**kwargs):
    try:
        gsc_files = kwargs['ti'].xcom_pull(task_ids='t1_list_files')
        last_status_files = kwargs['ti'].xcom_pull(task_ids='t2_check_files_statuses')

        if not gsc_files:
            logging.info("No files found in storage.")
            return False

        if not last_status_files:
            logging.info("No files found in BigQuery statuses.")
            return False

        not_processed_files_set = {file['filename'] for file in last_status_files if file['status'] != 'done'}
        if len(not_processed_files_set) == 0:
            logging.info("All files already proceeded. WELL DONE. >> cerveza time")
            return False
        data = []
        process_status = 0
        process_len = len(gsc_files)
        for file in gsc_files:
            process_status += 1
            logging.info(f'processed {process_status} from {process_len} channels')
            match = vars.MSGS_FILE_PATTERN.match(file['name'])
            if file['name'] in not_processed_files_set and match:
                logging.info(f"File {file['name']} ready to load to BQ.")
            elif file['name'] in not_processed_files_set:
                logging.info(f"Filename {file['name']} does not match the expected pattern.")
                continue
            else:
                # logging.info(f"Filename {file['name']} already loaded to BQ. keep calm.")
                continue

            uploaded = upload_msg_bq(path=file['full_path'])
            if uploaded:
                row = json_helper.make_row_msg_status(file, match, status='done')
                data.append(row)

        if not data:
            logging.info("No data to save.")
            return False

        json_helper.save_to_line_delimited_json(data, TMP_FILE)
        logging.info(f"Temporary file with uploaded msg)files created at {TMP_FILE}")
        return TMP_FILE
    except Exception as e:
        raise Exception(f'Error processing files: {e}')


def update_status_table(file_path):
    try:
        logging.info(f"Uploading file {file_path} to BigQuery.")
        return bigquery_manager.load_json_to_bigquery(
            json_file_path=file_path,
            table_id=f'{vars.BIGQUERY_DATASET}.{vars.BIGQUERY_UPLOAD_STATUS_TABLE}'
        )
    except Exception as e:
        raise Exception(f'Error updating status table: {e}')


def clear_tmp_files(file_path):
    try:
        os.remove(file_path)
        logging.info(f'Temporary file {file_path} deleted')
    except OSError as e:
        logging.error(f'Error deleting temporary file {file_path}: {e}')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 23, 0, 0, 0),
}

with DAG(
        'f1_load_msg_to_bq',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
        tags=['f1', 'message-processing', 'bigquery', 'gcs', 'tg_jobs_parser']
) as dag:
    list_raw_msgs_files = PythonOperator(
        task_id='t1_list_files',
        python_callable=list_msgs,
    )

    check_files_statuses = PythonOperator(
        task_id='t2_check_files_statuses',
        python_callable=check_files_statuses,
    )

    upload_msgs_to_bq = ShortCircuitOperator(
        task_id='t3_process_task',
        provide_context=True,
        python_callable=process_all_files,
    )

    update_status_table = PythonOperator(
        task_id='t4_update_status_table',
        python_callable=update_status_table,
        op_kwargs={'file_path': "{{ ti.xcom_pull(task_ids='t3_process_task') }}"}
    )

    clear_tmp_files = PythonOperator(
        task_id='t5_clear_tmp_files_task',
        python_callable=clear_tmp_files,
        op_kwargs={'file_path': "{{ ti.xcom_pull(task_ids='t3_process_task') }}"}
    )

    list_raw_msgs_files >> check_files_statuses >> upload_msgs_to_bq
    upload_msgs_to_bq >> update_status_table >> clear_tmp_files

if __name__ == '__main__':
    dag.test()
