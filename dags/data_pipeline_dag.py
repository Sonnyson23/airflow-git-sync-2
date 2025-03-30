from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests
import boto3

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
    schedule=None,  # Trigger manually or via CI/CD
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dataops'],
) as dag:

    # 1. Create 'traindb' database in PostgreSQL
    create_database = PostgresOperator(
        task_id='create_traindb_database',
        postgres_conn_id='postgresql_conn',
        sql="SELECT 'CREATE DATABASE traindb' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'traindb');",
        autocommit=True
    )

    # 2. Create the transactions table
    create_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgresql_conn',
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
        # Download CSV file
        url = "https://raw.githubusercontent.com/erkansirin78/datasets/refs/heads/master/dirty_store_transactions.csv"
        response = requests.get(url)
        data = response.content

        # Upload to MinIO using boto3
        s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='dataopsadmin',
            aws_secret_access_key='dataopsadmin'
        )

        bucket_name = 'dataops-bronze'
        key = 'raw/dirty_store_transactions.csv'

        # Create bucket if it doesn't exist
        existing_buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
        if bucket_name not in existing_buckets:
            s3.create_bucket(Bucket=bucket_name)

        s3.put_object(Bucket=bucket_name, Key=key, Body=data)
        return "Data uploaded to MinIO"

    upload_data = PythonOperator(
        task_id='upload_data_to_minio',
        python_callable=download_and_upload_to_minio,
    )

    # 4. Download PostgreSQL JDBC driver on Spark container
    setup_spark_client = SSHOperator(
        task_id='setup_spark_client',
        ssh_conn_id='spark_ssh_conn',
        command="""
        mkdir -p /dataops
        cd /dataops
        if [ ! -f postgresql-42.6.0.jar ]; then
            wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
        fi
        """,
    )

    # 5. Clean and transform data using PySpark on remote container
    clean_data = SSHOperator(
        task_id='clean_transform_data',
        ssh_conn_id='spark_ssh_conn',
        command="""
        cd /dataops
        mkdir -p /dataops/scripts
        pip install pandas pyspark boto3

        cat > /dataops/scripts/data_cleaning.py << 'EOL'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, trim

def clean_data():
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
        df = spark.read.csv(
            "s3a://dataops-bronze/raw/dirty_store_transactions.csv",
            header=True,
            inferSchema=True
        )

        clean_df = df.dropDuplicates() \\
            .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")) \\
            .withColumn("store_id", trim(col("store_id"))) \\
            .withColumn("customer_id", trim(col("customer_id"))) \\
            .withColumn("product_id", trim(col("product_id"))) \\
            .withColumn("product_category", trim(col("product_category"))) \\
            .withColumn("amount", when(col("amount").isNull(), 0.0).otherwise(col("amount"))) \\
            .withColumn("payment_method", when(col("payment_method").isNull(), "Unknown").otherwise(trim(col("payment_method"))))

        clean_df.write \\
            .format("jdbc") \\
            .option("url", "jdbc:postgresql://postgres:5432/traindb") \\
            .option("dbtable", "public.clean_data_transactions") \\
            .option("user", "airflow") \\
            .option("password", "airflow") \\
            .option("driver", "org.postgresql.Driver") \\
            .mode("overwrite") \\
            .save()

    except Exception as e:
        print("Error:", str(e))
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    clean_data()
EOL

        python3 /dataops/scripts/data_cleaning.py
        """,
    )

    # Define task dependencies
    create_database >> create_table >> upload_data >> setup_spark_client >> clean_data
    # Note: The SSH connection ID 'spark_ssh_conn' should be defined in Airflow connections