from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'hadoop',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 27),
    'email': ['irachide1@gmail.com'],
    'email_on_failure': False,
    'retries': 0,
}

dag = DAG(
    'match_histories_dag',
    default_args=default_args,
    description='DAG to fetch match histories',
    schedule_interval=None,
    catchup=False,
)

fetch_match_histories_task = BashOperator(
    task_id='fetch_match_histories',
    bash_command='python3 /home/hadoop/Desktop/projectBD/scripts/fetch_match_histories.py ',
    dag=dag,
)

trigger_history_format_dag = TriggerDagRunOperator(
    task_id='trigger_history_format_dag',
    trigger_dag_id='history_format_dag',
    dag=dag,
)

fetch_match_histories_task >> trigger_history_format_dag
