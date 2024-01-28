from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.operators.dummy_operator import DummyOperator  # Not needed
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'hadoop',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 27),  # Adjust as needed
    'email': ['irachide1@gmail.com'],
    'email_on_failure': False,
    'retries': 0,
}

dag = DAG(
    'match_id_dag',
    default_args=default_args,
    description='DAG to fetch match IDs',
    schedule_interval=None,  # This DAG should be triggered by the previous DAG
)

fetch_match_ids_task = BashOperator(
    task_id='fetch_match_ids',
    bash_command='python3 /home/hadoop/Desktop/projectBD/scripts/fetch_match_ids.py ',
    dag=dag,
)

# If you have a next DAG to trigger, you can add TriggerDagRunOperator here
# Otherwise, you can end the DAG with fetch_match_ids_task

# Example of a trigger for the next DAG (if any)
# trigger_next_dag = TriggerDagRunOperator(
#     task_id='trigger_next_dag',
#     trigger_dag_id='next_dag_id',  # Replace with your next DAG ID
#     dag=dag,
# )

# fetch_match_ids_task >> trigger_next_dag  # If you have a next DAG to trigger
