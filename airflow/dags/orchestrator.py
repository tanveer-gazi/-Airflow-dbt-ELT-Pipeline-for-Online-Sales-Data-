import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

sys.path.append('/opt/airflow/request-script')

def safe_main_callable():
    from load_data import main
    return main()

default_args = {
    'description': 'A DAG to orchestrate data',
    'start_date': datetime(2025, 8, 20),
    'catchup': False
}

dag = DAG(
    dag_id='online-sales-ingest-dbt-orchestrator',
    default_args=default_args,
    schedule=timedelta(days=1)
)

with dag:
    # Task 1
    ingest_data = PythonOperator(
        task_id='ingest_data',
        python_callable=safe_main_callable
    )
    # Task 2
    data_transform = DockerOperator(
        task_id='transform_data_task',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='run',
        working_dir='/usr/app',
        mounts=[
            Mount(source='E:/Works/DE Project/online-sales-data-pipeline/dbt/my_project',
                  target='/usr/app',
                  type='bind'),
            Mount(source='E:/Works/DE Project/online-sales-data-pipeline/dbt/profiles.yml',
                  target='/root/.dbt/profiles.yml',
                  type='bind')
        ],
        network_mode='online-sales-data-pipeline_my_network',
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
        auto_remove='success'
    )

    ingest_data >> data_transform