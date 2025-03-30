from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, trim, regexp_replace
import os

def clean_data():
    # Spark session oluşturma
    spark = SparkSession.builder \
        .appName("Data Cleaning") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "dataops") \
        .config("spark.hadoop.fs.s3a.secret.key", "Ankara06") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars", "/tmp/dataops/postgresql-42.6.0.jar") \
        .getOrCreate()
    
    # MinIO'dan veriyi okuma
    try:
        df = spark.read.csv(
            "s3a://dataops-bronze/raw/dirty_store_transactions.csv",
            header=True,
            inferSchema=True
        )
        
        print("Veri şeması:")
        df.printSchema()
        print(f"Orijinal veri satır sayısı: {df.count()}")
        
        # Veri temizleme işlemleri
        clean_df = df \
            .dropDuplicates() \
            .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")) \
            .withColumn("store_id", trim(col("store_id"))) \
            .withColumn("customer_id", trim(col("customer_id"))) \
            .withColumn("product_id", trim(col("product_id"))) \
            .withColumn("product_category", trim(col("product_category"))) \
            .withColumn("amount", 
                        when(col("amount").isNull(), 0.0)
                        .otherwise(col("amount"))) \
            .withColumn("payment_method", 
                       when(col("payment_method").isNull(), "Unknown")
                       .otherwise(trim(col("payment_method"))))
        
        print(f"Temizlenmiş veri satır sayısı: {clean_df.count()}")
        
        # PostgreSQL'e yazma
        jdbc_url = "jdbc:postgresql://postgres:5432/traindb"
        
        clean_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "public.clean_data_transactions") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        print("Veri başarıyla temizlendi ve PostgreSQL'e yazıldı.")
    
    except Exception as e:
        print(f"Hata oluştu: {str(e)}")
    finally:
        # Spark session'ı kapatma
        spark.stop()

if __name__ == "__main__":
    clean_data()