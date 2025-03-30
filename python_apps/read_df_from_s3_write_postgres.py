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
df = load_df_from_s3(bucket='airflow-demo', key='dataops12/ml/Churn_Modelling.csv', s3=s3)
print(df.head())
# Do the necessary operations like transformation, join, clean and filter.

# Write pandas dataframe to postgresql table
df.to_sql('churn_modelling', con=engine, if_exists='replace')