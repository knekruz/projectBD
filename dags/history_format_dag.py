from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
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
    'history_format_dag',
    default_args=default_args,
    description='DAG to format match histories',
    schedule_interval=None,  # This DAG should be triggered by the previous DAG
    catchup=False,
)

format_history_task = BashOperator(
    task_id='format_history',
    bash_command='python3 /home/hadoop/Desktop/projectBD/spark/history_format.py',
    dag=dag,
)

# Trigger for the next DAG
trigger_elastic_load_dag = TriggerDagRunOperator(
    task_id='trigger_elastic_load_dag',
    trigger_dag_id='elastic_load_dag',  # Replace with your next DAG ID
    dag=dag,
)

format_history_task >> trigger_elastic_load_dag
