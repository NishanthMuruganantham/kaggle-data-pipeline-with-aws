import json
import logging
import os
import zipfile
from typing import List, Set
import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError
from mens_t20i_data_collector._lambdas.constants import (
    CRICSHEET_DATA_DOWNLOADING_URL,
    CRICSHEET_DATA_S3_FOLDER_NAME,
    CRICSHEET_DATA_S3_FOLDER_TO_STORE_PROCESSED_JSON_FILES_ZIP
)
from mens_t20i_data_collector._lambdas.utils import (
    get_environmental_variable_value
)

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DownloadDataFromCricsheetHandler:

    def __init__(self) -> None:
        self._cricsheet_url = CRICSHEET_DATA_DOWNLOADING_URL
        self._sns_topic_arn = get_environmental_variable_value("SNS_TOPIC_ARN")
        self._s3_bucket_name = get_environmental_variable_value("DOWNLOAD_BUCKET_NAME")
        self._sns_client = boto3.client("sns")
        self._s3_client = boto3.client("s3")
        self._temp_folder: str = "/tmp"
        self._extraction_directory: str = f"{self._temp_folder}/extracted_files"
        self._s3_folder_to_store_cricsheet_data: str = CRICSHEET_DATA_S3_FOLDER_NAME
        self._s3_folder_to_store_processed_json_files_zip: str = CRICSHEET_DATA_S3_FOLDER_TO_STORE_PROCESSED_JSON_FILES_ZIP

    def download_data_from_cricsheet(self) -> str:
        try:
            logger.info(f"Starting download from {self._cricsheet_url}")
            response = requests.get(self._cricsheet_url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to download data from Cricsheet: {e}")
            raise

        zip_file_name = os.path.basename(self._cricsheet_url)
        zip_file_path = f"{self._temp_folder}/{zip_file_name}"

        try:
            with open(zip_file_path, "wb") as file:
                file.write(response.content)
            logger.info(f"File downloaded successfully from cricsheet and placed in {zip_file_path}")
            return zip_file_path
        except IOError as e:
            logger.error(f"Failed to write downloaded data to {zip_file_path}: {e}")
            raise

    def upload_new_json_data_files_for_data_processing(self, downloaded_zip_file_path: str):

        try:
            with zipfile.ZipFile(downloaded_zip_file_path, "r") as zip_file_content:
                zip_file_content.extractall(self._extraction_directory)
            logger.info(f"Downloaded zip file is extracted successfully to {self._extraction_directory}")

        except zipfile.BadZipFile as e:
            logger.error(f"Failed to extract the downloaded zip file: {e}")
            raise

        new_files = self._seggregate_new_files_from_downloaded_zip()
        self._upload_new_json_files_to_s3_and_send_sns_notification(new_files=new_files)


    def _list_all_the_available_items_in_processed_folder(self) -> Set:
        processed_files_folder_path = f"{self._s3_folder_to_store_cricsheet_data}/{self._s3_folder_to_store_processed_json_files_zip}/"
        try:
            list_bucket_objects_response = self._s3_client.list_objects_v2(
                Bucket=self._s3_bucket_name,
                Prefix=processed_files_folder_path
            )
            if "Contents" in list_bucket_objects_response:
                return set(os.path.basename(obj["Key"]) for obj in list_bucket_objects_response["Contents"] if obj["Key"].endswith(".json"))
            return set()
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to get the list of processed files from {processed_files_folder_path}: {e}")
            raise

    def _seggregate_new_files_from_downloaded_zip(self) -> List:
        new_files: List = []
        processed_files: Set = self._list_all_the_available_items_in_processed_folder()
        logger.info(f"Available processed files: {processed_files}")

        for _, _, files in os.walk(self._extraction_directory):
            for file in files:
                if file.endswith(".json"):
                    if file not in processed_files:
                        new_files.append(file)
        logger.info(f"Newly downloaded files: {new_files}")
        return new_files

    def _send_sns_notification_with_json_file_key(self, json_file_key: str):
        logger.info(f"Sending SNS notification with {json_file_key}")
        json_message_body = {
            "json_file_key": json_file_key,
            "match_id": os.path.splitext(os.path.basename(json_file_key))[0]
        }
        sns_response = self._sns_client.publish(
            Subject="New Cricsheet Data JSON File",
            Message=json.dumps(
                json_message_body, indent=4, sort_keys=True, default=str
            ),
            TopicArn=self._sns_topic_arn,
        )
        logger.info(f"SNS response: {sns_response}")

    def _upload_new_json_files_to_s3_and_send_sns_notification(self, new_files: List):
        for file in new_files[:2]:
            file_path = f"{self._extraction_directory}/{file}"
            key = f"{self._s3_folder_to_store_cricsheet_data}/{self._s3_folder_to_store_processed_json_files_zip}/{file}"
            self._s3_client.upload_file(Bucket=self._s3_bucket_name, Key=key, Filename=file_path)
            logger.info(f"File {file} uploaded to {key}")
            self._send_sns_notification_with_json_file_key(key)


def handler(_, __):     # noqa: Vulture
    try:
        downloader = DownloadDataFromCricsheetHandler()
        zip_file_path = downloader.download_data_from_cricsheet()
        downloader.upload_new_json_data_files_for_data_processing(zip_file_path)

        logging.shutdown()
        return {
            "statusCode": 200,
            "body": "Data downloaded and placed successfully for processing"
        }
    except Exception as e:      # pylint: disable=broad-exception-caught
        logger.error(f"Handler execution failed: {e}")
        logging.shutdown()
        return {
            "statusCode": 500,
            "body": f"Internal Server Error: {str(e)}"
        }
