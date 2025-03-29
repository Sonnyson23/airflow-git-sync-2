from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, sys
from airflow import DAG
import pandas as pd

start_date = datetime(2023, 10, 11)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

def add(**kwargs):
    x = kwargs['x']
    y = kwargs['y']
    return x + y

def multiply(**kwargs):
    x = kwargs['x']
    y = kwargs['y']
    return x * y

def divide(**kwargs):
    x = kwargs['x']
    y = kwargs['y']
    return x / y

with DAG('calculations', default_args=default_args, schedule_interval='*/5 * * * *', catchup=False) as dag:

    t1 = PythonOperator(task_id='add', python_callable=add,
                        op_kwargs={'x': 20, 'y': 30 })

    t2 = PythonOperator(task_id='multiply', python_callable=multiply,
                        op_kwargs={'x': 20, 'y': 5 })
    
    t3 = PythonOperator(task_id='divide', python_callable=divide,
                        op_kwargs={'x': 20, 'y': 5 })
    t1 >> t2 >> t3