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

TMP_FILE = os.path.join(volume_folder_path, 'tmp_not_uploaded_msg_files.json')


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


def check_files_statuses():
    if check_table_exists():
        return ctx.bigquery_manager.query_msg_files_statuses(
            dataset_id=vars.BIGQUERY_DATASET,
            table_id=vars.BIGQUERY_UPLOAD_STATUS_TABLE,
        )
    else:
        return []


def process_all_files(**context):
    gsc_files = context['task_instance'].xcom_pull(task_ids='t1_list_files')
    last_status_files = context['task_instance'].xcom_pull(task_ids='t2_check_not_processed')
    if not last_status_files:
        print("No files found.")
        return

    not_processed_files_set = {file['filename'] for file in last_status_files if file['status'] != 'done'}
    try:
        data = []
        for file in gsc_files:
            match = vars.MSGS_FILE_PATTERN.match(file['name'])
            if file['name'] in not_processed_files_set and match:
                print(f"File {file['name']} ready to load to BQ, ")
            else:
                print(f"Filename {file['name']} does not match the expected pattern.")
                continue

            uploaded = upload_msg_bq(path=file['full_path'])
            if uploaded:
                row = json_helper.make_row_msg_status(file, match, status='done')
                data.append(row)
        print(f"looks like all file age uploaded to BQ well, GOOD JOB MAZAFAKER")
    except Exception as e:
        print(f"Error uploading file to BQ: {e}")
    finally:
        # save done status to tmp file
        if data:
            json_helper.save_to_line_delimited_json(data, TMP_FILE)
            return TMP_FILE
        else:
            return


def update_status_table(file_path):
    if not file_path:
        logging.info('No files to process. Be happy!')
        return

    logging.info(f"Uploading file {file_path} to BigQuery.")
    return ctx.bigquery_manager.load_json_to_bigquery(
        json_file_path=file_path,
        table_id=f'{vars.BIGQUERY_DATASET}.{vars.BIGQUERY_UPLOAD_STATUS_TABLE}'
    )


def upload_msg_bq(path):
    if path is None or path == 'None':
        logging.info('No files to process. Be happy!')
        return

    return ctx.bigquery_manager.load_json_uri_to_bigquery(
        path=path,
        table_id=f'{vars.BIGQUERY_DATASET}.{vars.BIGQUERY_RAW_MESSAGES_TABLE}'
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
        'f1_load_msg_to_bq',
        default_args=default_args,
        schedule_interval=None,  # Этот DAG запускается только по триггеру
        start_date=days_ago(1),
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

    upload_msgs_to_bq = PythonOperator(
        task_id='t3_process_task',
        provide_context=True,
        python_callable=process_all_files,
    )

    update_status_table = PythonOperator(
        task_id='t4_start_files_process_task',
        python_callable=update_status_table,
        op_kwargs={'file_path': "{{ ti.xcom_pull(task_ids='t3_process_task') }}"}
    )

    clear_tmp_files = PythonOperator(
        task_id='t5_clear_tmp_files_task',
        python_callable=clear_tmp_files,
        op_kwargs={'file_path': "{{ ti.xcom_pull(task_ids='t3_process_task') }}"}
    )

    list_raw_msgs_files >> check_files_statuses >> upload_msgs_to_bq >> update_status_table >> clear_tmp_files


if __name__ == '__main__':
    dag.test()
