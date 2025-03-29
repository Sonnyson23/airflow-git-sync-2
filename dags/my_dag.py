from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

start_date = datetime(2023, 10, 11)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('my_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    t0 = BashOperator(task_id='ls_data', bash_command='ls -l /tmp', retries=2, retry_delay=timedelta(seconds=15))

    t1 = BashOperator(task_id='download_data',
                      bash_command='curl -L -o /tmp/dirty_store_transactions.csv https://github.com/erkansirin78/datasets/raw/master/dirty_store_transactions.csv',
                      retries=2, retry_delay=timedelta(seconds=15))

    t2 = BashOperator(task_id='check_file_exists', bash_command='sha256sum /tmp/dirty_store_transactions.csv',
                      retries=2, retry_delay=timedelta(seconds=15))

    t0 >> t1 >> t2