import io
import logging
import boto3
import pandas as pd
from mens_t20i_data_collector._lambdas.constants import (
    CRICSHEET_DATA_S3_OUTPUT_FOLDER,
    DELIVERYWISE_DATA_CSV_FILE_NAME,
    DELIVERYWISE_DATAFRAME_COLUMNS
)
from mens_t20i_data_collector._lambdas.utils import (
    get_environmental_variable_value
)

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DatasetPreparationHandler:

    """Handler to read data from DynamoDB, format it as a DataFrame, and upload as CSV to S3."""

    def __init__(self) -> None:
        self._deliverywise_data_dynamo_db_table = get_environmental_variable_value("DYNAMODB_TO_STORE_DELIVERYWISE_DATA")
        self._s3_bucket_name = get_environmental_variable_value("DOWNLOAD_BUCKET_NAME")
        self._s3_client = boto3.client("s3")
        self._s3_resource = boto3.resource("s3")
        self._dynamodb_resource = boto3.resource('dynamodb')

    def read_data_in_dynamodb_and_convert_to_dataframe(self):
        deliverywise_dataframe = self._read_data_from_dynamo_db(self._deliverywise_data_dynamo_db_table)
        deliverywise_dataframe.sort_values(by=["match_id", "innings_number", "over_number", "ball_number"], inplace=True)
        formatted_deliverywise_dataframe: pd.DataFrame = deliverywise_dataframe[DELIVERYWISE_DATAFRAME_COLUMNS]     # type: ignore
        self._convert_dataframe_to_csv_and_upload_to_s3(formatted_deliverywise_dataframe, DELIVERYWISE_DATA_CSV_FILE_NAME)

    def _read_data_from_dynamo_db(self, table_name: str) -> pd.DataFrame:
        logger.info(f"Fetching data from DynamoDB table '{table_name}'.")

        try:
            table = self._dynamodb_resource.Table(table_name)   # type: ignore
            response = table.scan()
            data = response.get('Items', [])
            while 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                data.extend(response.get('Items', []))

            if not data:
                logger.warning("No data found in DynamoDB table.")

            return pd.DataFrame(data)

        except Exception as e:
            logger.error(f"Error fetching data from DynamoDB: {str(e)}", exc_info=True)
            raise

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

def handler(_, __):     # noqa: Vulture
    logger.info("Lambda function invoked.")
    try:
        dataset_preparation_handler = DatasetPreparationHandler()
        dataset_preparation_handler.read_data_in_dynamodb_and_convert_to_dataframe()
        return {
            'statusCode': 200,
            'body': 'Dataset successfully processed and uploaded.'
        }
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(f"Lambda function error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': f"Error processing dataset: {str(e)}"
        }
