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
    'match_histories_dag',
    default_args=default_args,
    description='DAG to fetch match histories',
    schedule_interval=None,  # This DAG should be triggered by the previous DAG
)

fetch_match_histories_task = BashOperator(
    task_id='fetch_match_histories',
    bash_command='python3 /home/hadoop/Desktop/projectBD/scripts/fetch_match_histories.py ',
    dag=dag,
)

fetch_match_histories_task
