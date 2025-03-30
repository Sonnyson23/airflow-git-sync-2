from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, trim, regexp_replace
import os

def clean_data():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Data Cleaning") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "dataops") \
        .config("spark.hadoop.fs.s3a.secret.key", "Ankara06") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars", "/tmp/dataops/postgresql-42.6.0.jar") \
        .getOrCreate()
    
    try:
        # Load raw data from MinIO
        df = spark.read.csv(
            "s3a://dataops-bronze/raw/dirty_store_transactions.csv",
            header=True,
            inferSchema=True
        )
        
        print("Raw schema:")
        df.printSchema()
        print(f"Original row count: {df.count()}")

        # Drop duplicates
        df = df.dropDuplicates()

        # Remove non-alphanumeric characters from some fields
        df = df.withColumn("store_id", regexp_replace(col("store_id"), "[^a-zA-Z0-9]", "")) \
               .withColumn("product_category", regexp_replace(col("product_category"), "[^a-zA-Z0-9 ]", "")) \
               .withColumn("product_id", regexp_replace(col("product_id"), "[^0-9]", ""))

        # Trim all columns
        for column in df.columns:
            df = df.withColumn(column, trim(col(column)))

        # Format date and handle nulls
        df = df.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")) \
               .withColumn("amount", when(col("amount").isNull(), 0.0).otherwise(col("amount"))) \
               .withColumn("payment_method", when(col("payment_method").isNull(), "Unknown").otherwise(col("payment_method")))

        print(f"Cleaned row count: {df.count()}")

        # Write cleaned data to PostgreSQL
        jdbc_url = "jdbc:postgresql://postgres:5432/traindb"
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "public.clean_data_transactions") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        print("Cleaned data successfully written to PostgreSQL.")

    except Exception as e:
        print(f"Error occurred: {str(e)}")

    finally:
        spark.stop()

if __name__ == "__main__":
    clean_data()
