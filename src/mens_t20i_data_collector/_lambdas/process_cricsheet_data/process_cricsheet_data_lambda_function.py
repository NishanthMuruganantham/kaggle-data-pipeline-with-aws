import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, __):
    try:
        logger.info(f"Received event: {event}")
        return {
            "statusCode": 200,
            "body": "Data Processed successfully"
        }
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": f"Internal Server Error: {str(e)}"
        }
