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
    'summoner_id_dag',
    default_args=default_args,
    description='DAG to fetch summoner IDs',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

fetch_summoner_ids = BashOperator(
    task_id='fetch_summoner_ids',
    bash_command='python3 /home/hadoop/Desktop/projectBD/scripts/fetch_summoner_ids.py ',
    dag=dag,
)

trigger_match_id_dag = TriggerDagRunOperator(
    task_id='trigger_match_id_dag',
    trigger_dag_id='match_id_dag',
    dag=dag,
)

fetch_summoner_ids >> trigger_match_id_dag
