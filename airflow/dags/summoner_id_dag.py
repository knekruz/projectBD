from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'hadoop',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 27),
    'email': ['nekruzk@3il.fr'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'summoner_id_dag',
    default_args=default_args,
    description='A simple DAG to fetch summoner IDs',
    schedule_interval=timedelta(days=1),
)

t1 = BashOperator(
    task_id='fetch_summoner_ids',
    bash_command='python3 /home/hadoop/Desktop/projectBD/scripts/fetch_summoner_ids.py ',
    dag=dag,
    execution_timeout=timedelta(minutes=1),  # Set a reasonable timeout
)

t1
