import os
import requests
import logging
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from mens_t20i_data_collector._lambdas.constants import (
    CRICSHEET_DATA_DOWNLOADING_URL,
    CRICSHEET_DATA_S3_FOLDER_NAME
)


# Setup logging
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class DownloadDataFromCricsheetHandler:
    def __init__(self) -> None:
        self._cricsheet_url = CRICSHEET_DATA_DOWNLOADING_URL
        self._s3_bucket_name = os.getenv('DOWNLOAD_BUCKET_NAME')
        self._s3_client = boto3.client('s3')
        self._s3_folder_name = CRICSHEET_DATA_S3_FOLDER_NAME

        if not self._s3_bucket_name:
            logger.error("Environment variable 'DOWNLOAD_BUCKET_NAME' is missing.")
            raise ValueError("Missing required environment variable: 'DOWNLOAD_BUCKET_NAME'")

    def download_data_from_cricsheet(self) -> None:
        try:
            logger.info(f"Starting download from {self._cricsheet_url}")
            response = requests.get(self._cricsheet_url)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to download data from Cricsheet: {e}")
            raise

        zip_file_name = os.path.basename(self._cricsheet_url)
        zip_file_path = f"/tmp/{zip_file_name}"

        try:
            with open(zip_file_path, 'wb') as file:
                file.write(response.content)
            logger.info(f"File downloaded successfully to {zip_file_path}")
        except IOError as e:
            logger.error(f"Failed to write downloaded data to {zip_file_path}: {e}")
            raise

        try:
            self._s3_client.upload_file(
                Filename=zip_file_path,
                Bucket=self._s3_bucket_name,
                Key=f"{self._s3_folder_name}/{zip_file_name}"
            )
            logger.info(f"File uploaded successfully to S3 bucket {self._s3_bucket_name} with key {self._s3_folder_name}/{zip_file_name}")
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to upload file to S3: {e}")
            raise


def handler(event, context):
    try:
        downloader = DownloadDataFromCricsheetHandler()
        downloader.download_data_from_cricsheet()
        response_message = "File downloaded and uploaded to S3 successfully."
        logger.info(response_message)
        logging.shutdown()
        return {
            'statusCode': 200,
            'body': response_message
        }
    except Exception as e:
        logger.error(f"Handler execution failed: {e}")
        logging.shutdown()
        return {
            'statusCode': 500,
            'body': f"Internal Server Error: {str(e)}"
        }
