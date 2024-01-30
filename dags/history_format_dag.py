# history_format_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG('history_format_dag',
          default_args=default_args,
          description='A simple DAG to run PySpark script',
          schedule_interval=timedelta(days=1))

t1 = BashOperator(
    task_id='run_pyspark',
    bash_command='spark-submit /home/hadoop/Desktop/projectBD/spark/history_format.py',
    dag=dag,
)

t1
