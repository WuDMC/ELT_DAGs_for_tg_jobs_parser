import logging
import os
from airflow import DAG
from google.api_core.exceptions import NotFound
from tg_jobs_parser.google_cloud_helper.storage_manager import StorageManager
from tg_jobs_parser.google_cloud_helper.bigquery_manager import BigQueryManager
from tg_jobs_parser.configs import vars, volume_folder_path
from tg_jobs_parser.utils import json_helper
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

TMP_FILE = os.path.join(volume_folder_path, 'tmp_uploaded_msg_difference.json')


class Context:
    def __init__(self):
        self.storage_manager = StorageManager()
        self.bigquery_manager = BigQueryManager(vars.PROJECT_ID)


ctx = Context()


def list_msgs():
    return ctx.storage_manager.list_msgs_with_metadata()


def check_table_exists():
    try:
        ctx.bigquery_manager.client.get_table(f"{vars.BIGQUERY_DATASET}.{vars.BIGQUERY_UPLOAD_STATUS_TABLE}")
        return True
    except NotFound:
        return False


def check_uploaded_files():
    if check_table_exists():
        return ctx.bigquery_manager.fetch_data(
            dataset_id=vars.BIGQUERY_DATASET,
            table_id=vars.BIGQUERY_UPLOAD_STATUS_TABLE,
            selected_fields='filename'
        )
    else:
        return []


def download_and_process_files(**context):
    gsc_files = context['task_instance'].xcom_pull(task_ids='list_files')
    bigquery_files = context['task_instance'].xcom_pull(task_ids='check_uploaded')
    if not gsc_files:
        print("No files found.")
        return

    existing_files_set = set(file['filename'] for file in bigquery_files)
    data = []
    for file in gsc_files:
        if file['name'] in existing_files_set:
            print(f"File {file['name']} already exists in BigQuery. Skipping.")
            continue

        match = vars.MSGS_FILE_PATTERN.match(file['name'])
        if not match:
            print(f"Filename {file['name']} does not match the expected pattern.")
            continue

        row = json_helper.make_row_msg_inprocess(file, match)
        data.append(row)
    if not data:
        print("No new files to process.")
        return

    # Save data to a temporary file
    try:
        json_helper.save_to_line_delimited_json(data, TMP_FILE)
    except Exception as e:
        print(f"Error creating temporary file: {e}")
        return

    return TMP_FILE


def upload_to_bq(file_path):
    if file_path is None or file_path == 'None':
        logging.info('No files to process. Be happy!')
        return

    return ctx.bigquery_manager.load_json_to_bigquery(
        json_file_path=file_path,
        table_id=f'{vars.BIGQUERY_DATASET}.{vars.BIGQUERY_UPLOAD_STATUS_TABLE}'
    )


def clear_tmp_files(file_path):
    if file_path is None or file_path == 'None':
        logging.info('No files to process. Be happy!')
        return

    try:
        os.remove(file_path)
        print(f'Temporary file {file_path} deleted')
    except OSError as e:
        print(f'Error: {e}')


with DAG(
    'process_raw_msg_files',
    default_args=default_args,
    schedule_interval='15 */6 * * *',
    catchup=False,
    max_active_runs=1,
) as dag:
    list_raw_msgs_files = PythonOperator(
        task_id='list_files',
        python_callable=list_msgs,
    )

    check_uploaded_msg_files = PythonOperator(
        task_id='check_uploaded',
        python_callable=check_uploaded_files,
    )

    get_files_to_process = PythonOperator(
        task_id='get_files_to_process_task',
        provide_context=True,
        python_callable=download_and_process_files,
    )

    start_files_process = PythonOperator(
        task_id='start_files_process_task',
        python_callable=upload_to_bq,
        op_kwargs={'file_path': "{{ task_instance.xcom_pull(task_ids='get_files_to_process_task') }}"}
    )

    clear_tmp_files = PythonOperator(
        task_id='clear_tmp_files_task',
        python_callable=clear_tmp_files,
        op_kwargs={'file_path': "{{ task_instance.xcom_pull(task_ids='get_files_to_process_task') }}"}
    )

    list_raw_msgs_files >> check_uploaded_msg_files >> get_files_to_process >> start_files_process >> clear_tmp_files

if __name__ == '__main__':
    dag.test()
