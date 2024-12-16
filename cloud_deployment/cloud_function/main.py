import base64
import functions_framework
import os
import json
from google.cloud import secretmanager, storage
from io import BytesIO

from typing import Any
import composer2_airflow_rest_api

def trigger_dag_gcf(data, context=None):
    web_server_url = (
        "https://cf213c8557b54c59999c32e8bb4cb940-dot-us-central1.composer.googleusercontent.com"
    )
    # Replace with the ID of the DAG that you want to run.
    dag_id = 'gcs_to_bq_ingestion'

    composer2_airflow_rest_api.trigger_dag(web_server_url, dag_id, data)


# GCS Bucket variables
BUCKET_NAME = "data_load_kaggle"
GCS_DESTINATION = "supermarket_sales.csv"  

def access_secret(secret_name, version="latest"):
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/5524937409/secrets/{secret_name}/versions/{version}"
    response = client.access_secret_version(request={"name": secret_path})
    secret_payload = response.payload.data.decode("UTF-8")
    return secret_payload

def upload_to_gcs(bucket_name, destination_blob_name, file_content):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_file(file_content)
    print(f"File uploaded to gs://{bucket_name}/{destination_blob_name}")

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def fetch_and_upload_kaggle_data(cloud_event):
    try:
        # Kaggle connection
        print("Accessing the secrets")
        kaggle_username = access_secret("kaggle_username")
        kaggle_key = access_secret("kaggle_key")

        print(kaggle_username)

        os.environ["KAGGLE_USERNAME"] = kaggle_username
        os.environ["KAGGLE_KEY"] = kaggle_key
        import kaggle

        # Download the dataset from Kaggle
        kaggle.api.dataset_download_files("aungpyaeap/supermarket-sales", path="/tmp", unzip=True)

        print("Downloaded the file")

        # Read the downloaded file into memory (Assuming the dataset is in a .csv format)
        local_file_path = "/tmp/supermarket_sales - Sheet1.csv"
        
        with open(local_file_path, "rb") as file:
            file_content = BytesIO(file.read())

        # Upload the CSV file to GCS
        upload_to_gcs(BUCKET_NAME, GCS_DESTINATION, file_content)

        print("Uploaded to Kaggle!")

        data = {"check":"check1"}
        trigger_dag_gcf(data)
        return "Uploaded to Kaggle!"

    except Exception as e:
        print(f"Error: {str(e)}")
        return f"Error: {str(e)}"