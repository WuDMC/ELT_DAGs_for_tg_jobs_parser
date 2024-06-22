import logging
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from google.api_core.exceptions import NotFound
from tg_jobs_parser.google_cloud_helper.storage_manager import StorageManager
from tg_jobs_parser.google_cloud_helper.bigquery_manager import BigQueryManager
from tg_jobs_parser.configs import vars, volume_folder_path
from tg_jobs_parser.utils import json_helper
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

TMP_FILE = os.path.join(volume_folder_path, 'tmp_not_processed_msg_files.json')


class Context:
    def __init__(self):
        self.storage_manager = StorageManager()
        self.bigquery_manager = BigQueryManager(vars.PROJECT_ID)


ctx = Context()


def list_msgs():
    logging.info("Listing messages from storage.")
    return ctx.storage_manager.list_msgs_with_metadata()


def check_table_exists():
    try:
        logging.info(f"Checking if table {vars.BIGQUERY_DATASET}.{vars.BIGQUERY_UPLOAD_STATUS_TABLE} exists.")
        ctx.bigquery_manager.client.get_table(f"{vars.BIGQUERY_DATASET}.{vars.BIGQUERY_UPLOAD_STATUS_TABLE}")
        return True
    except NotFound:
        logging.warning("Table not found.")
        return False


def check_msg_files_in_process():
    if check_table_exists():
        logging.info("Fetching uploaded files from BigQuery.")
        return ctx.bigquery_manager.fetch_data(
            dataset_id=vars.BIGQUERY_DATASET,
            table_id=vars.BIGQUERY_UPLOAD_STATUS_TABLE,
            selected_fields='filename'
        )
    else:
        logging.warning("No uploaded files to fetch.")
        return []


def download_and_process_files(**kwargs):
    gsc_files = kwargs['ti'].xcom_pull(task_ids='t1_list_files')
    bigquery_files = kwargs['ti'].xcom_pull(task_ids='t2_check_uploaded')

    if not gsc_files:
        logging.info("No files found in storage.")
        return

    existing_files_set = set(file['filename'] for file in bigquery_files)
    data = []
    for file in gsc_files:
        if file['name'] in existing_files_set:
            logging.info(f"File {file['name']} already exists in BigQuery. Skipping.")
            continue

        match = vars.MSGS_FILE_PATTERN.match(file['name'])
        if not match:
            logging.warning(f"Filename {file['name']} does not match the expected pattern.")
            continue

        row = json_helper.make_row_msg_status(file, match, status='in process')
        data.append(row)

    if not data:
        logging.info("No new files to process.")
        return

    # Save data to a temporary file
    try:
        json_helper.save_to_line_delimited_json(data, TMP_FILE)
        logging.info(f"Temporary file created at {TMP_FILE}")
    except Exception as e:
        logging.error(f"Error creating temporary file: {e}")
        return

    return TMP_FILE


def update_status_table(file_path):
    if not file_path:
        logging.info('No files to process. Be happy!')
        return

    logging.info(f"Uploading file {file_path} to BigQuery.")
    return ctx.bigquery_manager.load_json_to_bigquery(
        json_file_path=file_path,
        table_id=f'{vars.BIGQUERY_DATASET}.{vars.BIGQUERY_UPLOAD_STATUS_TABLE}'
    )


def clear_tmp_files(file_path):
    if not file_path:
        logging.info('No files to process. Be happy!')
        return

    try:
        os.remove(file_path)
        logging.info(f'Temporary file {file_path} deleted')
    except OSError as e:
        logging.error(f'Error: {e}')


with DAG(
        'f1_msgs_to_process',
        default_args=default_args,
        schedule_interval='15 */6 * * *',
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

    process_new_files = PythonOperator(
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
        wait_for_completion=False,  # Не ждем завершения DAG-а
        execution_date='{{ execution_date }}',  # Передаем execution_date
    )

    list_raw_msgs_files >> check_msg_files_in_process >> process_new_files >> update_status_table >> clear_tmp_files >> trigger_f1_load_msg_to_bq

if __name__ == '__main__':
    dag.test()
