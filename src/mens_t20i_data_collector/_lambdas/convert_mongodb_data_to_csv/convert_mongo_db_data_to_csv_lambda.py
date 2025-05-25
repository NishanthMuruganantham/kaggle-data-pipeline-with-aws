import io
import logging
import boto3
import pandas as pd
from pymongo import MongoClient
from mens_t20i_data_collector._lambdas.constants import (
    CRICSHEET_DATA_S3_OUTPUT_FOLDER,
    DELIVERYWISE_DATA_CSV_FILE_NAME,
    MATCHWISE_DATA_CSV_FILE_NAME
)
from mens_t20i_data_collector._lambdas.utils import (
    exception_handler,
    get_environmental_variable_value
)

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DatasetPreparationHandler:

    """Handler to read data from DynamoDB, format it as a DataFrame, and upload as CSV to S3."""

    def __init__(self) -> None:
        self._s3_client = boto3.client("s3")
        self._s3_resource = boto3.resource("s3")
        self._s3_bucket_name = get_environmental_variable_value("DOWNLOAD_BUCKET_NAME")
        self._mongo_db_url = get_environmental_variable_value("MONGO_DB_URL")
        self._mongo_db_name = get_environmental_variable_value("MONGO_DB_NAME")
        self._matchwise_data_collection_name = get_environmental_variable_value("MATCHWISE_DATA_COLLECTION_NAME")
        self._deliverywise_data_collection_name = get_environmental_variable_value("DELIVERYWISE_DATA_COLLECTION_NAME")
        self._mongo_db_client = MongoClient(self._mongo_db_url)
        self._matchwise_data_mongo_collection = self._mongo_db_client[self._mongo_db_name][self._matchwise_data_collection_name]
        self._deliverywise_data_mongo_collection = self._mongo_db_client[self._mongo_db_name][self._deliverywise_data_collection_name]

    @property
    def matchwise_data(self):
        logger.info("Preparing matchwise data.")
        matchwise_data_cursor = self._matchwise_data_mongo_collection.find()
        matchwise_dataframe: pd.DataFrame = pd.DataFrame(matchwise_data_cursor)
        matchwise_dataframe.drop(columns=["_id"], inplace=True)
        matchwise_dataframe.rename(columns={"index": "match_number"}, inplace=True)
        matchwise_dataframe.sort_values(by=["date", "match_id"], inplace=True)
        matchwise_dataframe["match_number"] = range(1, len(matchwise_dataframe) + 1)
        return matchwise_dataframe

    @property
    def deliverywise_data(self):
        logger.info("Preparing deliverywise data.")
        deliverywise_data_cursor = self._deliverywise_data_mongo_collection.find()
        deliverywise_dataframe: pd.DataFrame = pd.DataFrame(deliverywise_data_cursor)
        deliverywise_dataframe.drop(columns=["_id", "composite_delivery_key"], inplace=True)
        deliverywise_dataframe = deliverywise_dataframe.merge(self.matchwise_data[["match_number", "match_id"]], on="match_id", how="left")
        deliverywise_dataframe.sort_values(by=["match_number", "innings_number", "over_number", "ball_number"], inplace=True)
        return deliverywise_dataframe

    def prepare_dataset(self):
        logger.info("Preparing dataset for matchwise data.")
        self._convert_dataframe_to_csv_and_upload_to_s3(self.matchwise_data, MATCHWISE_DATA_CSV_FILE_NAME)
        logger.info("Preparing dataset for deliverywise data.")
        self._convert_dataframe_to_csv_and_upload_to_s3(self.deliverywise_data, DELIVERYWISE_DATA_CSV_FILE_NAME)

    def _convert_dataframe_to_csv_and_upload_to_s3(self, dataframe: pd.DataFrame, filename: str):
        logger.info(f"Converting DataFrame to '{filename}' and uploading to '{self._s3_bucket_name}'")
        try:
            csv_buffer = io.StringIO()
            dataframe.to_csv(csv_buffer, index=False)
            self._s3_resource.Object(self._s3_bucket_name, f"{CRICSHEET_DATA_S3_OUTPUT_FOLDER}/{filename}").put(    # type: ignore
                Body=csv_buffer.getvalue()
            )
            logger.info(f"CSV file '{filename}' uploaded to S3 successfully.")

        except Exception as e:
            logger.error(f"Failed to upload '{filename}' to S3: {str(e)}", exc_info=True)
            raise


@exception_handler  # noqa: Vulture
def handler(_, __):     # noqa: Vulture
    """
    Lambda function handler to convert MongoDB data to CSV and upload to S3.
    """
    dataset_preparation_handler = DatasetPreparationHandler()
    dataset_preparation_handler.prepare_dataset()
    return "Datasets prepared and uploaded to S3 successfully."
