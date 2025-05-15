import json
import logging
import os
import tempfile
import boto3
import pandas as pd
from mens_t20i_data_collector._lambdas.constants import (
    CRICSHEET_DATA_S3_OUTPUT_FOLDER,
    DELIVERYWISE_DATA_CSV_FILE_NAME,
    MATCHWISE_DATA_CSV_FILE_NAME
)
from mens_t20i_data_collector._lambdas.utils import (
    exception_handler,
    get_environmental_variable_value
)

# Set up Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class KaggleDatasetUploader:

    """Handler to upload dataset to Kaggle."""

    def __init__(self):
        self._temporary_directory = tempfile.gettempdir()
        self._s3_bucket_name = get_environmental_variable_value("DOWNLOAD_BUCKET_NAME")
        self._kaggle_username = get_environmental_variable_value("KAGGLE_USERNAME")
        self._create_kaggle_json_file()
        self._matchwise_data_csv_file_s3_key = f"{CRICSHEET_DATA_S3_OUTPUT_FOLDER}/{MATCHWISE_DATA_CSV_FILE_NAME}"
        self._deliverywise_data_csv_file_s3_key = f"{CRICSHEET_DATA_S3_OUTPUT_FOLDER}/{DELIVERYWISE_DATA_CSV_FILE_NAME}"
        self._folder_to_keep_the_files_to_upload = os.path.join(self._temporary_directory, "files_to_upload_to_kaggle")
        self._s3_client = boto3.client("s3")

    def upload_dataset_to_kaggle(self):
        """
        Uploads the dataset to Kaggle.
        """
        os.makedirs(self._folder_to_keep_the_files_to_upload, exist_ok=True)
        self._create_metadata_json_file()
        self._download_dataset_files_from_s3()
        self._authenticate_to_kaggle_and_upload_dataset()

    def _authenticate_to_kaggle_and_upload_dataset(self):
        """
        Authenticates to Kaggle and uploads the dataset.
        """
        try:
            logger.info("Authenticating to Kaggle and uploading dataset...")
            from kaggle.api.kaggle_api_extended import \
                KaggleApi  # pylint: disable=import-outside-toplevel
            api = KaggleApi()
            api.authenticate()
            logger.info("Kaggle authentication successful")
            logger.info("Uploading dataset to Kaggle...")
            last_match_details = self._get_last_match_details()
            team_1 = last_match_details["team_1"]
            team_2 = last_match_details["team_2"]
            date = last_match_details["date"]
            api.dataset_create_version(
                delete_old_versions=True,
                folder=self._folder_to_keep_the_files_to_upload,
                version_notes=f"Dataset updated till the match between {team_1} and {team_2} on {date}",
            )
            logger.info("Dataset uploaded to Kaggle successfully")
        except Exception as e:
            logger.error(f"Error occurred while uploading dataset to Kaggle: {e}", exc_info=True)
            raise

    def _create_kaggle_json_file(self):
        """
        Creates a kaggle.json file with the username and key for Kaggle API authentication.
        """
        logger.info("Creating kaggle.json file...")
        kaggle_json = {
            "username": self._kaggle_username,
            "key": get_environmental_variable_value("KAGGLE_SECRET_KEY"),
        }
        kaggle_json_file_path = f"{self._temporary_directory}/kaggle.json"
        with open(kaggle_json_file_path, "w", encoding="utf-8") as kaggle_json_file:
            kaggle_json_file.write(json.dumps(kaggle_json))
        os.environ["KAGGLE_CONFIG_DIR"] = self._temporary_directory
        logger.info(f"kaggle.json file created at the temporary path {kaggle_json_file_path}")

    def _create_metadata_json_file(self):
        """
        Creates a metadata.json file with the dataset metadata for Kaggle API.
        """
        logger.info("Creating metadata.json file...")
        metadata = {
            "id": f"{self._kaggle_username}/{get_environmental_variable_value('KAGGLE_DATASET_SLUG')}",
        }
        metadata_file_path = os.path.join(self._folder_to_keep_the_files_to_upload, "dataset-metadata.json")
        with open(metadata_file_path, "w", encoding="utf-8") as metadata_file:
            metadata_file.write(json.dumps(metadata))
        logger.info(f"metadata.json file created at the temporary path {metadata_file_path}")

    def _download_dataset_files_from_s3(self):
        """
        Downloads the dataset files from S3.
        """
        logger.info("Downloading dataset files from S3...")
        self._s3_client.download_file(
            self._s3_bucket_name,
            self._matchwise_data_csv_file_s3_key,
            os.path.join(self._folder_to_keep_the_files_to_upload, MATCHWISE_DATA_CSV_FILE_NAME)
        )
        self._s3_client.download_file(
            self._s3_bucket_name,
            self._deliverywise_data_csv_file_s3_key,
            os.path.join(self._folder_to_keep_the_files_to_upload, DELIVERYWISE_DATA_CSV_FILE_NAME)
        )
        logger.info("Dataset files downloaded from S3")

    def _get_last_match_details(self):
        """
        Gets the last match details from the matchwise data.
        """
        logger.info("Getting last match details...")
        matchwise_data = pd.read_csv(
            os.path.join(self._folder_to_keep_the_files_to_upload, MATCHWISE_DATA_CSV_FILE_NAME)
        )
        last_match_details = matchwise_data.iloc[-1]
        logger.info(f"Last match details: {last_match_details}")
        return last_match_details.to_dict()


@exception_handler      # noqa: Vulture
def handler(_, __):
    """
    Lambda function handler to upload dataset to Kaggle.
    """
    kaggle_uploader = KaggleDatasetUploader()
    kaggle_uploader.upload_dataset_to_kaggle()
    return "Dataset uploaded to Kaggle successfully"
