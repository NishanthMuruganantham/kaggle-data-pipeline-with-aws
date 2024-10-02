import logging

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def handler(_, __):     # noqa: Vulture
    return {
        "statusCode": 200,
        "body": "Hello from Lambda!",
    }
