import boto3, logging, botocore
from botocore.config import Config
import pandas as pd
import io
import argparse

ap = argparse.ArgumentParser()

ap.add_argument("-ep", "--endpoint_url", required=True, type=str, default='http://minio:9000',
                    help="MinIO rest URL. Default: http://minio:9000")
ap.add_argument("-aki", "--aws_access_key_id", required=True, type=str, default='dataops',
                    help="aws_access_key_id. Default: dataops")
ap.add_argument("-sak", "--aws_secret_access_key", required=True, type=str, default='Ankara06',
                    help="aws_secret_access_key. Default: Ankara06")
ap.add_argument("-sfu", "--source_file_url", required=True, type=str, default='https://github.com/erkansirin78/datasets/raw/master/Churn_Modelling.csv',
                    help="source_file_url. Default: https://github.com/erkansirin78/datasets/raw/master/Churn_Modelling.csv")

args = vars(ap.parse_args())

endpoint_url = args['endpoint_url']
aws_access_key_id = args['aws_access_key_id']
aws_secret_access_key = args['aws_secret_access_key']
source_file_url = args['source_file_url']

def get_s3_client():
    s3 = boto3.client('s3',
                      endpoint_url=endpoint_url,
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      config=Config(signature_version='s3v4'))
    return s3

def save_df_to_s3(df, bucket, key, s3):
    ''' Store file to s3''' 
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
        logging.info(f"{key} saved to s3 bucket {bucket}")
    except Exception as e:
        raise logging.exception(e)

s3 = get_s3_client()
df = pd.read_csv(source_file_url)
save_df_to_s3(df=df, bucket='airflow-demo', key='dataops12/ml/Churn_Modelling.csv', s3=s3)