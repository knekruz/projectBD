from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'hadoop',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 27),  # Adjust as needed
    'email': ['irachide1@gmail.com'],
    'email_on_failure': False,
    'retries': 0,
}

dag = DAG(
    'elastic_load_dag',
    default_args=default_args,
    description='DAG to load data into Elasticsearch',
    schedule_interval=None,  # This DAG is triggered by the previous DAG
    catchup=False,
)

load_into_elastic_task = BashOperator(
    task_id='load_into_elastic',
    bash_command='python3 /home/hadoop/Desktop/projectBD/spark/elastic_load.py',
    dag=dag,
)

# This is the final DAG in your pipeline, so it does not trigger another DAG
