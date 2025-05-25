import logging
import os
import zipfile
from typing import List, Set
import boto3
import requests
from mens_t20i_data_collector._lambdas.constants import (
    CRICSHEET_DATA_DOWNLOADING_URL,
    CRICSHEET_DATA_S3_FOLDER_NAME,
    CRICSHEET_DATA_S3_FOLDER_TO_STORE_PROCESSED_JSON_FILES_ZIP
)
from mens_t20i_data_collector._lambdas.utils import (
    exception_handler,
    get_environmental_variable_value
)

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DownloadDataFromCricsheetHandler:

    def __init__(self) -> None:
        self._cricsheet_url = CRICSHEET_DATA_DOWNLOADING_URL
        dynamodb_client = boto3.resource("dynamodb")
        self._dynamo_db_to_store_file_data_extraction_status = dynamodb_client.Table(   # type: ignore
            get_environmental_variable_value("DYNAMODB_TABLE_NAME")
        )
        self._s3_bucket_name = get_environmental_variable_value("DOWNLOAD_BUCKET_NAME")
        self._threshold_for_number_of_files_to_be_sent_for_processing = int(get_environmental_variable_value(
            "THRESHOLD_FOR_NUMBER_OF_FILES_TO_BE_SENT_FOR_PROCESSING"
        ))
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
        self._upload_new_json_files_to_s3(new_files=new_files)
        if new_files:
            self._trigger_an_sqs_message_whenever_new_file_is_downloaded(new_files=new_files)
            return "Data file has been downloaded and placed successfully for processing"
        logger.info("No new files to process")
        return "No new files to process"

    def _list_all_files_from_dynamo_db(self) -> Set:
        response = self._dynamo_db_to_store_file_data_extraction_status.scan(ProjectionExpression="file_name")
        return set(item["file_name"] for item in response["Items"])

    def _seggregate_new_files_from_downloaded_zip(self) -> List:
        new_files: List = []
        processed_files = self._list_all_files_from_dynamo_db()
        logger.info(f"Total available processed files = {len(processed_files)}")
        for _, _, files in os.walk(self._extraction_directory):
            for file in files:
                if file.endswith(".json"):
                    if file not in processed_files:
                        new_files.append(file)
        logger.info(f"Total newly downloaded files: {len(new_files)}")
        return new_files

    def _trigger_an_sqs_message_whenever_new_file_is_downloaded(self, new_files: List[str]):
        """
        This function will trigger an SQS message whenever a new file is downloaded
        :param new_files: List of new files downloaded
        :return: None
        """
        sqs_client = boto3.client("sqs")
        queue_url = get_environmental_variable_value("DELAYED_SQS_QUEUE_URL")
        message_body = {
            "message": "New files downloaded from Cricsheet",
            "new_files": new_files,
        }
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=str(message_body)
        )
        logger.info(f"Message sent to SQS: {response['MessageId']}")

    def _upload_new_json_files_to_s3(self, new_files: List):
        for file in new_files[:self._threshold_for_number_of_files_to_be_sent_for_processing]:
            file_path = f"{self._extraction_directory}/{file}"
            key = f"{self._s3_folder_to_store_cricsheet_data}/{self._s3_folder_to_store_processed_json_files_zip}/{file}"
            self._s3_client.upload_file(Bucket=self._s3_bucket_name, Key=key, Filename=file_path)
            logger.info(f"File {file} uploaded to {key}")


@exception_handler      # noqa: Vulture
def handler(_, __):
    downloader = DownloadDataFromCricsheetHandler()
    zip_file_path = downloader.download_data_from_cricsheet()
    output = downloader.upload_new_json_data_files_for_data_processing(zip_file_path)
    logging.shutdown()
    return output
