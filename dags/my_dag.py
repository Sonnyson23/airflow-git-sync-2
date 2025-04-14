from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email
from airflow.models import TaskInstance
from airflow.utils.state import State

start_date = datetime(2025, 3, 11)

def notify_email_on_failure(context):
    task_instance: TaskInstance = context.get('task_instance')
    dag_run = context.get('dag_run')
    log_url = task_instance.log_url  # UI'deki log linki
    subject = f"[Airflow] Hata: {task_instance.task_id} görevi başarısız oldu!"
    
    html_content = f"""
    <h3>Airflow Görev Hatası Bildirimi</h3>
    <p><strong>DAG:</strong> {task_instance.dag_id}</p>
    <p><strong>Görev:</strong> {task_instance.task_id}</p>
    <p><strong>Yürütme Tarihi:</strong> {context.get('execution_date')}</p>
    <p><strong>Hata Mesajı:</strong> {task_instance.try_number}. deneme başarısız.</p>
    <p><strong>Loglar:</strong> <a href="{log_url}">Görev Logları</a></p>
    """

    send_email(to=["ademsonuvar@gmail.com"], subject=subject, html_content=html_content)
default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    'my_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    on_failure_callback=notify_email_on_failure
) as dag:
    ...

    t0 = BashOperator(task_id='ls_data', 
                      bash_command='ls -l /tmp', retries=2, retry_delay=timedelta(seconds=15))

    t1 = BashOperator(task_id='download_data',
                      bash_command='curl -L -o /tmp/dirty_store_transactions.csv https://github.com/erkansirin78/datasets/raw/master/dirty_store_transactions.csv',
                      retries=2, retry_delay=timedelta(seconds=15))

    t2 = BashOperator(task_id='check_file_exists', bash_command='sha256sum /tmp/dirty_store_transactions.csv',
                      retries=2, retry_delay=timedelta(seconds=15))

    t0 >> t1 >> t2