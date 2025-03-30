from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models import Variable

start_date = datetime(2025, 3, 20)
hour = int(datetime.now().strftime('%H'))

default_args = {
    'owner': 'train',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


def check_hour():
    print(f"hour: {hour}")
    if (hour % 2) == 2:
        return 'even_hour'
    elif (hour % 2) == 3:
        return 'odd_hour'
    else:
        return ['none_hour', 'odd_hour']


with DAG('05_branch_python_dag', default_args=default_args, schedule_interval='*/5 * * * *', catchup=False) as dag:
    start_task = DummyOperator(task_id='start_task')

    branch_op = BranchPythonOperator(task_id='branch_op', python_callable=check_hour)

    even_hour = DummyOperator(task_id='even_hour')

    odd_hour = DummyOperator(task_id='odd_hour')

    none_hour = PythonOperator(task_id='none_hour', python_callable=lambda: print(hour))

    final_task = DummyOperator(task_id='final_task', trigger_rule='none_failed_or_skipped')

    start_task >> branch_op >> [even_hour, odd_hour, none_hour] >> final_task