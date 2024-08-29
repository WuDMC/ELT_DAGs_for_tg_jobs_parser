from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# Определяем параметры DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

# Параметры для загрузки из GCS в BigQuery
BUCKET_NAME = "wu-eu-west"
SOURCE_OBJECT = "test/test2.txt"  # Файл или паттерн для файлов


# Функция для вывода файлов, найденных сенсором
def log_files_found(bucket_name, source_object, **kwargs):
    blobs = kwargs["ti"].xcom_pull(task_ids="wait_for_file")
    for blob in blobs:
        print(f"Found file: {blob.name}")


# Создаем DAG
with DAG(
    "test_sensor",
    default_args=default_args,
    description="Load data from GCS to BigQuery",
    schedule_interval=None,
    catchup=False,
    tags=["test"],
) as dag:

    # Ожидание появления нового файла в GCS
    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_file",
        bucket=BUCKET_NAME,
        object=SOURCE_OBJECT,
        google_cloud_conn_id="G_CLOUD",
    )

    # Задача для вывода найденных файлов в логи
    log_files_task = PythonOperator(
        task_id="log_files_task",
        python_callable=log_files_found,
        op_args=[BUCKET_NAME, SOURCE_OBJECT],
        provide_context=True,
    )

    # Dummy задачи для наглядности
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # Определяем порядок задач
    start >> wait_for_file >> log_files_task >> end
