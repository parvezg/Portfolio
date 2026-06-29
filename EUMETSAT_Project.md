The objective of this project is to extract an JSON weather API and store is S3 partition. Automate this workflow pipeline with Apache Airflow.

## Airflow Weather ETL Pipeline

```python
# libraries and file paths
import requests
import json
import logging
import sys
import boto3
import ast

from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path

sys.path.append(Path("/var/lib/airflow/").as_posix())  # Path where HashiCorp Vault helper is saved.
from data.hashicorp import HCVaultSecretManager  # Import credentials from HashiCorp Vault


# -------------------------
# Extract weather data
# -------------------------

def weather_api(**context):

    logging.info("Starting API request")

    api_url = "https://api.open-meteo.com/v1/forecast"

    parameters = {
        "latitude": 52.52,
        "longitude": 13.41,
        "hourly": "temperature_2m",
        "models": "icon_seamless",
    }

    try:
        response = requests.get(api_url, params=parameters, timeout=30)

        logging.info(f"API response code: {response.status_code}")

        response.raise_for_status()

        file = response.json()

        logging.info("Data successfully retrieved")

        results = []

        latitude = file["latitude"]
        longitude = file["longitude"]
        hourly_data = file["hourly"]["time"]
        temperatures = file["hourly"]["temperature_2m"]

        for i, h in enumerate(hourly_data):
            date = h[:10]
            hour = h[-5:]

            results.append(
                {
                    "date": date,
                    "time": hour,
                    "temperature": temperatures[i],
                }
            )

        logging.info(f"Latitude: {latitude}, Longitude: {longitude}")
        logging.info(f"Total records: {len(results)}")

        context["ti"].xcom_push(
            key="weather_results",
            value=results,
        )

    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error: {e}")
        raise

    except requests.exceptions.Timeout as e:
        logging.error(f"API request timed out: {e}")
        raise

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        raise

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise


# -------------------------
# Upload data to Amazon S3
# -------------------------

def s3_connection(**context):

    results = context["ti"].xcom_pull(
        task_ids="Extracting_from_api",
        key="weather_results",
    )

    if not results:
        raise ValueError("No results received from weather_api task")

    logging.info(f"Records received: {len(results)}")

    sm = HCVaultSecretManager()
    aws_creds = sm.get_variable("variable_aws")

    if isinstance(aws_creds, str):
        aws_creds = ast.literal_eval(aws_creds)

    s3 = boto3.client(
        "s3",
        region_name="eu-central-1",
        aws_access_key_id=aws_creds["accesskey"],
        aws_secret_access_key=aws_creds["secretkey"],
    )

    logging.info("S3 client created successfully")

    today = datetime.now(timezone.utc)

    s3_key = (
        f"Parvez/"
        f"weather/"
        f"year={today.year}/"
        f"month={today.month:02d}/"
        f"day={today.day:02d}/"
        f"forecast.json"
    )

    s3.put_object(
        Bucket="vx-playground",
        Key=s3_key,
        Body=json.dumps(results, indent=2),
        ContentType="application/json",
    )

    logging.info(f"Data uploaded to S3://<bucket>/{s3_key}")
    logging.info(f"Records uploaded: {len(results)}")


# -------------------------
# Airflow DAG
# -------------------------

default_args = {
    "owner": "airflow",
    "email": ["parvez.gundumalli@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="My_First_dag_test",
    default_args=default_args,
    start_date=datetime(2026, 5, 7),
    schedule="@daily",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="Extracting_from_api",
        python_callable=weather_api,
        retries=3,
        retry_delay=timedelta(minutes=5),
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id="Upload_to_s3",
        python_callable=s3_connection,
        retries=3,
        retry_delay=timedelta(minutes=5),
        provide_context=True,
    )

    extract_task >> upload_task
```
