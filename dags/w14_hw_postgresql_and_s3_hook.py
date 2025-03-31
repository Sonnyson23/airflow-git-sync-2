from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os, sys
from airflow import DAG

start_date = datetime(2023, 10, 11)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

endpoint_url = Variable.get("endpoint_url")
aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")
source_file_url = "https://github.com/erkansirin78/datasets/raw/master/Churn_Modelling.csv"
ssh_train_password = Variable.get("ssh_train_password")


with DAG('s3_to_postgres_dag', default_args=default_args, schedule_interval='@once', catchup=False) as dag:
    
    t0 = BashOperator(
    task_id="scp_python_scrips",
    bash_command=f"sshpass -v -p {ssh_train_password} scp -o StrictHostKeyChecking=no -r /opt/airflow/code_base/airflow-git-sync-2/python_apps ssh_train@spark_client:/home/ssh_train/")
    
    t1 = SSHOperator(
    task_id="df_to_s3",
    command=f"""source /dataops/airflowenv/bin/activate && 
    python /home/ssh_train/python_apps/w14_hw_load_df_to_s3.py -ep {endpoint_url} -aki {aws_access_key_id} -sak {aws_secret_access_key} -sfu {source_file_url}""",
    ssh_conn_id='spark_ssh_conn')

    t2 = SSHOperator(task_id='s3_to_postgres', 
                    command="""source /dataops/airflowenv/bin/activate &&
                    python /home/ssh_train/python_apps/w14_hw_read_df_from_s3_write_postgres.py """,
                    ssh_conn_id='spark_ssh_conn')

    t0 >> t1 >> t2
    # 