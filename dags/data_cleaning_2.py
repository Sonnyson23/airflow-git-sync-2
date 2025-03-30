from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, trim, regexp_replace, round
from pyspark.sql.types import IntegerType, FloatType

def clean_data():
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
        df = spark.read.csv(
            "s3a://dataops-bronze/raw/dirty_store_transactions.csv",
            header=True,
            inferSchema=False
        )

        df = df.dropDuplicates()

        # Clean categorical/text columns
        df = df.withColumn("STORE_ID", regexp_replace(col("STORE_ID"), r"[^a-zA-Z0-9]", "")) \
               .withColumn("STORE_LOCATION", regexp_replace(col("STORE_LOCATION"), r"[^a-zA-Z0-9 ]", "")) \
               .withColumn("PRODUCT_CATEGORY", regexp_replace(col("PRODUCT_CATEGORY"), r"[^a-zA-Z0-9 ]", "")) \
               .withColumn("PRODUCT_ID", regexp_replace(col("PRODUCT_ID"), r"[^0-9]", ""))

        # Clean and cast monetary columns
        for c in ["MRP", "CP", "DISCOUNT", "SP"]:
            df = df.withColumn(c, round(regexp_replace(col(c), r"[$,]", "").cast("float"), 2))

        # Trim all fields
        for column in df.columns:
            df = df.withColumn(column, trim(col(column)))

        # Cast types
        df = df.withColumn("PRODUCT_ID", col("PRODUCT_ID").cast(IntegerType())) \
               .withColumn("MRP", col("MRP").cast(FloatType())) \
               .withColumn("CP", col("CP").cast(FloatType())) \
               .withColumn("DISCOUNT", col("DISCOUNT").cast(FloatType())) \
               .withColumn("SP", col("SP").cast(FloatType())) \
               .withColumn("Date", to_date(col("Date"), "MM/dd/yyyy"))

        # Fill nulls if needed
        df = df.withColumn("DISCOUNT", when(col("DISCOUNT").isNull(), 0.0).otherwise(col("DISCOUNT"))) \
               .withColumn("SP", when(col("SP").isNull(), col("MRP") - col("DISCOUNT")).otherwise(col("SP")))

        print("Schema after cleaning:")
        df.printSchema()
        print("Cleaned row count:", df.count())

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

        print("✅ Data successfully saved to PostgreSQL.")

    except Exception as e:
        print("❌ Error:", str(e))
    finally:
        spark.stop()

if __name__ == "__main__":
    clean_data()
