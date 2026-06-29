
```python

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
from data.hashicorp import HCVaultSecretManager
import sys
import ast
import boto3
import ast 


sys.path.append(Path("/var/lib/airflow/").as_posix())     # converts python object into posix- sytle with forward slash /

def s3_test_connection ():
        sm= HCVaultSecretManager()
        aws_creds = sm.get_variable("var_aws")

        if isinstance ( aws_creds, str):                  # parse if string, use directly if alreday a dict
                aws_creds = ast.literal_eval(aws_creds)
        print("credentials are loaded...")

        s3 = boto3.client (
                's3',
                region_name = "eu-central-1" ,
                aws_access_key_id = aws_creds["accesskey"],
                aws_secret_access_key = aws_creds["secretkey"]

        )

        response = s3.list_buckets()

        print("Connected to s3 sucessfully!")

        print(response)
        

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["parvez.gundumalli@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}


with DAG(
  dag_id = "S3_Connection_Test",
  default_args= default_args,
  start_date = datetime(2026, 5, 11),
  schedule= None,
  catchup= False
  ) as dag:
        start = EmptyOperator(task_id='start')
        end = EmptyOperator(task_id='end')

        test_s3_connection = PythonOperator(
            task_id='extracting_aws_variable',
            python_callable = s3_test_connection
            
            )

        start >> test_s3_connection >> end

```
