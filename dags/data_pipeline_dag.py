from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
<<<<<<< HEAD
import boto3
=======
>>>>>>> 812715ed98f6a99a4d414151413f743b30878ae6
import io

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'store_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for store transactions data',
<<<<<<< HEAD
    schedule=None,  # Triggered manually or through CI/CD
=======
    schedule_interval=None,  # Triggered manually or through CI/CD
>>>>>>> 812715ed98f6a99a4d414151413f743b30878ae6
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dataops'],
) as dag:

    # 1. Create traindb database in Postgres
    create_database = PostgresOperator(
        task_id='create_traindb_database',
<<<<<<< HEAD
        postgres_conn_id= 'postgresql_conn',
=======
        postgres_conn_id= postgresql_conn,
>>>>>>> 812715ed98f6a99a4d414151413f743b30878ae6
        sql="SELECT 'CREATE DATABASE traindb' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'traindb');",
        autocommit=True
    )

    # 2. Create table in PostgreSQL
    create_table = PostgresOperator(
        task_id='create_postgres_table',
<<<<<<< HEAD
        postgres_conn_id= 'postgresql_conn',
=======
        postgres_conn_id= postgresql_conn,
>>>>>>> 812715ed98f6a99a4d414151413f743b30878ae6
        sql="""
        CREATE TABLE IF NOT EXISTS public.clean_data_transactions (
            transaction_id VARCHAR(255),
            transaction_date DATE,
            store_id VARCHAR(255),
            customer_id VARCHAR(255),
            product_id VARCHAR(255),
            product_category VARCHAR(255),
            amount NUMERIC(10, 2),
            payment_method VARCHAR(255)
        );
        """,
    )
    
    # 3. Download data and upload to MinIO
    def download_and_upload_to_minio():
<<<<<<< HEAD
        # Download data
        url = "https://raw.githubusercontent.com/erkansirin78/datasets/refs/heads/master/dirty_store_transactions.csv"
        response = requests.get(url)
        data = response.content

        # Connect to MinIO
        s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='dataopsadmin',
            aws_secret_access_key='dataopsadmin'
        )

        bucket_name = 'dataops-bronze'
        key = 'raw/dirty_store_transactions.csv'

        # Create bucket if not exists
        existing_buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
        if bucket_name not in existing_buckets:
            s3.create_bucket(Bucket=bucket_name)

        # Upload file
        s3.put_object(Bucket=bucket_name, Key=key, Body=data)

        return "Data uploaded to MinIO"

=======
        # Download the data
        url = "https://raw.githubusercontent.com/erkansirin78/datasets/refs/heads/master/dirty_store_transactions.csv"
        response = requests.get(url)
        data = response.content
        
        # Upload to MinIO
        s3_hook = S3Hook(aws_conn_id='minio_conn')
        s3_hook.load_bytes(
            bytes_data=data,
            key='raw/dirty_store_transactions.csv',
            bucket_name='dataops-bronze',
            replace=True
        )
        return "Data uploaded to MinIO"
    
    upload_data = PythonOperator(
        task_id='upload_data_to_minio',
        python_callable=download_and_upload_to_minio,
    )
>>>>>>> 812715ed98f6a99a4d414151413f743b30878ae6
    
    # 4. Download PostgreSQL driver on Spark client container
    setup_spark_client = SSHOperator(
        task_id='setup_spark_client',
        ssh_conn_id='spark_ssh',
        command="""
        mkdir -p /dataops
        cd /dataops
        if [ ! -f postgresql-42.6.0.jar ]; then
            wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
        fi
        """,
    )
    
    # 5. Clean and transform data (run on Spark client)
    clean_data = SSHOperator(
        task_id='clean_transform_data',
        ssh_conn_id='spark_ssh',
        command="""
        cd /dataops
        
        # Ensure scripts directory exists
        mkdir -p /dataops/scripts
        # Install dependencies for PySpark
        pip install pandas pyspark boto3

        # Write and execute the PySpark script
        cat > /dataops/scripts/data_cleaning.py << 'EOL'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, trim, regexp_replace
import os

def clean_data():
    # Create Spark session
    spark = SparkSession.builder \\
        .appName("Data Cleaning") \\
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \\
        .config("spark.hadoop.fs.s3a.access.key", "dataops") \\
        .config("spark.hadoop.fs.s3a.secret.key", "Ankara06") \\
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \\
        .config("spark.jars", "/dataops/postgresql-42.6.0.jar") \\
        .getOrCreate()
    
    try:
        # Read data from MinIO
        df = spark.read.csv(
            "s3a://dataops-bronze/raw/dirty_store_transactions.csv",
            header=True,
            inferSchema=True
        )
        
        print("Schema:")
        df.printSchema()
        print(f"Original row count: {df.count()}")
        
        # Perform data cleaning
        clean_df = df \\
            .dropDuplicates() \\
            .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")) \\
            .withColumn("store_id", trim(col("store_id"))) \\
            .withColumn("customer_id", trim(col("customer_id"))) \\
            .withColumn("product_id", trim(col("product_id"))) \\
            .withColumn("product_category", trim(col("product_category"))) \\
            .withColumn("amount", 
                        when(col("amount").isNull(), 0.0)
                        .otherwise(col("amount"))) \\
            .withColumn("payment_method", 
                       when(col("payment_method").isNull(), "Unknown")
                       .otherwise(trim(col("payment_method"))))
        
        print(f"Cleaned row count: {clean_df.count()}")
        
        # Write cleaned data to PostgreSQL
        jdbc_url = "jdbc:postgresql://postgres:5432/traindb"
        
        clean_df.write \\
            .format("jdbc") \\
            .option("url", jdbc_url) \\
            .option("dbtable", "public.clean_data_transactions") \\
            .option("user", "airflow") \\
            .option("password", "airflow") \\
            .option("driver", "org.postgresql.Driver") \\
            .mode("overwrite") \\
            .save()
        
        print("Data successfully cleaned and written to PostgreSQL.")
    
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    clean_data()
EOL

        python3 /dataops/scripts/data_cleaning.py
        """,
    )
    
    # Define DAG workflow
    create_database >> create_table >> upload_data >> setup_spark_client >> clean_data
