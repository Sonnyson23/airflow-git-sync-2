from typing import Optional
from sqlmodel import Field, SQLModel
from sqlmodel import create_engine
import pandas as pd
import boto3, logging, botocore
from botocore.config import Config
import io

def get_s3_client():
    s3 = boto3.client('s3',
                      endpoint_url='http://minio:9000',
                      aws_access_key_id='dataops',
                      aws_secret_access_key='Ankara06',
                      config=Config(signature_version='s3v4'))
    return s3

def load_df_from_s3(bucket, key, s3, sep=",", index_col=None, usecols=None):
    ''' Read a csv from a s3 bucket & load into pandas dataframe'''
    try:
        logging.info(f"Loading {bucket, key}")
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(obj['Body'], sep=sep, index_col=index_col, usecols=usecols, low_memory=False)
    except botocore.exceptions.ClientError as err:
        status = err.response["ResponseMetadata"]["HTTPStatusCode"]
        errcode = err.response["Error"]["Code"]
        if status == 404:
            logging.warning("Missing object, %s", errcode)
        elif status == 403:
            logging.error("Access denied, %s", errcode)
        else:
            logging.exception("Error in request, %s", errcode)

s3 = get_s3_client()
SQLALCHEMY_DATABASE_URL="postgresql://train:Ankara06@postgres:5432/traindb"
engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=True)

# Read csv file from minio and create pandas dataframe
df = load_df_from_s3(bucket='dataops-bronze', key='raw/dirty_store_transactions.csv', s3=s3)
print(df.head())

# Do the necessary operations like transformation, join, clean and filter.

# 1. Remove special characters from string columns
df['STORE_ID'] = df['STORE_ID'].str.replace(r'[^a-zA-Z0-9]', '', regex=True)
df['STORE_LOCATION'] = df['STORE_LOCATION'].str.replace(r'[^a-zA-Z0-9 ]', '', regex=True)
df['PRODUCT_CATEGORY'] = df['PRODUCT_CATEGORY'].str.replace(r'[^a-zA-Z0-9 ]', '', regex=True)

# 2. Clean PRODUCT_ID column.
df['PRODUCT_ID'] = df['PRODUCT_ID'].str.replace(r'[^0-9]', '', regex=True)

# 3. Remove special characters from currency columns, cast to float, and round to 2 decimal places
currency_columns = ["MRP", "CP", "DISCOUNT", "SP"]
for col_name in currency_columns:
    df[col_name] = df[col_name].str.replace(r'[$,]', '', regex=True).astype(float).round(2)

# 4. Remove leading/trailing whitespaces from all columns
for column in df.columns:
    if df[column].dtype == 'object':
        df[column] = df[column].str.strip()

# 5. Cast columns to appropriate data types
df['STORE_ID'] = df['STORE_ID'].astype(str)
df['STORE_LOCATION'] = df['STORE_LOCATION'].astype(str)
df['PRODUCT_CATEGORY'] = df['PRODUCT_CATEGORY'].astype(str)
df['PRODUCT_ID'] = pd.to_numeric(df['PRODUCT_ID'], errors='coerce').astype('Int64')
df['MRP'] = df['MRP'].astype(float)
df['CP'] = df['CP'].astype(float)
df['DISCOUNT'] = df['DISCOUNT'].astype(float)
df['SP'] = df['SP'].astype(float)
df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')  # Assuming the date format is year-month-day


# Write pandas dataframe to postgresql table
df.to_sql('clean_data_transactions', con=engine, if_exists='replace')
print("Data written to PostgreSQL table successfully.")