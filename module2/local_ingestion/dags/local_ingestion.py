import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from docker.types import Mount
from pathlib import Path



AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
HOST_DATA_PATH = os.environ.get("HOST_DATA_PATH", "")
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
#URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_2024-03.parquet'#{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_2023-{{ execution_date.strftime(\'%m\') }}.parquet'
#OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/data/output_2024-03.parquet'#{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/data/output_2023-{{ execution_date.strftime(\'%m\') }}.parquet'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    "LocalIngestionDag",
    default_args=default_args,
    schedule_interval="0 6 2 * *",  # At 06:00 on day-of-month 2
    catchup=False,
)

with dag:
    curl_task = BashOperator(
    task_id="curl",
    bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    ingest_task = DockerOperator(
        task_id='debug_environment',
        image='local_ingestion-ingest-data',
        #command='python /app/ingest_script.py --input_file /app/data/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet --db_host local-postgres --db_port 5432 --db_name localdata --db_user datauser --db_password datapass',
        #command='python /app/ingest_script.py --input_file /app/data/output_2024-03.parquet --db_host local-postgres --db_port 5432 --db_name localdata --db_user datauser --db_password datapass',
        command='python /app/ingest_script.py --input_file /app/data/output_2023-{{ execution_date.strftime(\'%m\') }}.parquet --db_host local-postgres --db_port 5432 --db_name localdata --db_user datauser --db_password datapass',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-network',
        api_version='auto',
        mount_tmp_dir=False,
        mounts=[Mount(
            target=f'/app/data',
            source=HOST_DATA_PATH,
            type='bind'
        )],
        auto_remove=True,
        dag=dag
    )
    curl_task >> ingest_task