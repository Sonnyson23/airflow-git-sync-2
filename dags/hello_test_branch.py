from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

start_date = datetime(2025, 3, 11)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('hello_test_branch', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    t0 = BashOperator(task_id='hellow_world', bash_command='echo hello World!!!', retries=2, retry_delay=timedelta(seconds=15))