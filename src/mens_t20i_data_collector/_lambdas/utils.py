import datetime
import functools
import logging
import os
from typing import Any
import requests
from botocore.exceptions import ClientError
from mens_t20i_data_collector._lambdas.constants import (
    TELEGRAM_MESSAGE_TEMPLATE
)

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def exception_handler(function):
    """
    Decorator to wrap the handler with a try-except block and provides error logging and a standardized response.
    """
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        current_time = datetime.datetime.now()
        function_name = "unknown_function"
        for arg in args:
            if hasattr(arg, "function_name"):
                function_name = arg.function_name
                break
        try:
            response_body = function(*args, **kwargs)
            send_alert_via_telegram_bot(
                chat_id=get_environmental_variable_value("TELEGRAM_CHAT_ID"),
                message=TELEGRAM_MESSAGE_TEMPLATE.format(
                    current_time.strftime("%d-%m-%Y"),
                    current_time.strftime("%H:%M:%S"),
                    function_name,
                    "SUCCESS ✅",
                    response_body
                ),
                telegram_bot_token=get_environmental_variable_value("TELEGRAM_BOT_TOKEN")
            )
            return {
                "statusCode": 200,
                "body": response_body
            }
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(f"Error occurred: {str(e)}", exc_info=True)
            send_alert_via_telegram_bot(
                chat_id=get_environmental_variable_value("TELEGRAM_CHAT_ID"),
                message=TELEGRAM_MESSAGE_TEMPLATE.format(
                    current_time.strftime("%d-%m-%Y"),
                    current_time.strftime("%H:%M:%S"),
                    function_name,
                    "ERROR ❌",
                    str(e)
                ),
                telegram_bot_token=get_environmental_variable_value("TELEGRAM_BOT_TOKEN")
            )
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


def send_alert_via_telegram_bot(chat_id: str,  message: str, telegram_bot_token: str, ) -> None:
    """
    Sends the statsu of the function execution through an alert to a Telegram chat.

    :param telegram_bot_token: Telegram bot token
    :param chat_id: Chat ID
    :param message: Message to send in HTML format
    """
    url = f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    response = requests.post(url, json=payload, timeout=10)
    if response.status_code != 200:
        print(f"Failed to send message: {response.text}")
