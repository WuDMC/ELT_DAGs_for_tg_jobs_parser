from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

GCS_BUCKET_NAME = 'wu-eu-west'

def print_files(**context):
    files = context['task_instance'].xcom_pull(task_ids='list_files')
    if not files:
        print("No files found.")
    else:
        print("Files found:")
        for file in files:
            print(file)

with DAG(
    'test_gcloud_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['test']
) as dag:

    # Задача для получения списка файлов
    list_files = GCSListObjectsOperator(
        task_id='list_files',
        bucket=GCS_BUCKET_NAME,
        prefix='job_parser/msgs',  # Добавьте префикс, если необходимо
        delimiter=None,
        gcp_conn_id='G_CLOUD'
    )

    # Задача для вывода списка файлов в лог
    print_files_task = PythonOperator(
        task_id='print_files',
        provide_context=True,
        python_callable=print_files,
    )

    list_files >> print_files_task

if __name__ == '__main__':
    dag.test()
