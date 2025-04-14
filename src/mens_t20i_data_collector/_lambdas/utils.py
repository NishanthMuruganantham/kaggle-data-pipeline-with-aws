import functools
import json
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


def parse_sns_event_message(function):
    """
    Decorator to parse the SNS event and passes the json_file_key and match_id to the decorated handler function.
    """
    @functools.wraps(function)
    def wrapper(event, _):
        logger.info(f"Received event: {event}")
        sns_message_body = event["Records"][0]["body"]
        json_file_s3_key_to_be_processed = json.loads(sns_message_body)["Message"]
        message_body = json.loads(json_file_s3_key_to_be_processed)
        json_file_key = message_body["json_file_key"]
        match_id = int(message_body["match_id"])
        logger.info(f"JSON file key to be processed: {json_file_key}")
        logger.info(f"Match ID: {match_id}")
        return function(json_file_key, match_id)

    return wrapper
