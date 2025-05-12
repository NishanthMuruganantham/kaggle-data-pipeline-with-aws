import functools
import logging
import os
from typing import Any
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def exception_handler(function):
    """
    Decorator to wrap the handler with a try-except block and provides error logging and a standardized response.
    """
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        try:
            response_body = function(*args, **kwargs)
            return {
                "statusCode": 200,
                "body": response_body
            }
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(f"Error occurred: {str(e)}", exc_info=True)
            return {
                "statusCode": 500,
                "body": f"Internal Server Error: {str(e)}"
            }
    return wrapper


def get_environmental_variable_value(variable_name: str) -> Any:
    value = os.getenv(variable_name)
    if value is None:
        logger.error(f"Environmental variable {variable_name} is not set")
        raise ValueError(f"Environmental variable {variable_name} is not set")
    return value


def make_dynamodb_entry_for_file_data_extraction_status(table, file_name: str, field: str, status: bool):
    """
    Creates a DynamoDB entry for file data extraction status.
    """
    try:
        response = table.update_item(
            Key={"file_name": file_name},
            UpdateExpression=f"set {field} = :val",
            ExpressionAttributeValues={":val": status},
            ReturnValues="UPDATED_NEW"
        )
        logger.info(f"Updated DynamoDB entry: {response}")
        return
    except ClientError as e:
        logger.error(f"Failed to update DynamoDB entry: {e.response['Error']['Message']}")
        raise


def parse_eventbridge_event_message(function):
    """
    Decorator to parse the EventBridge event and passes the json_file_key and match_id to the decorated handler function.
    """
    @functools.wraps(function)
    def wrapper(event, _):
        logger.info(f"Received event: {event}")
        s3_bucket_name = event["detail"]["bucket"]["name"]
        json_file_key = event["detail"]["object"]["key"]
        match_id = int(os.path.splitext(os.path.basename(json_file_key))[0])
        logger.info(f"S3 bucket name: {s3_bucket_name}")
        logger.info(f"JSON file key: {json_file_key}")
        return function(json_file_key, match_id)

    return wrapper
