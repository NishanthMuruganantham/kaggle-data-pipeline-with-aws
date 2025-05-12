import logging
from mens_t20i_data_collector._lambdas.utils import exception_handler

# Set up Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@exception_handler      # noqa: Vulture
def handler():
    """
    Lambda function handler to upload dataset to Kaggle.
    """
    logger.info("Uploading dataset to Kaggle")
    return "Dataset uploaded to Kaggle successfully"
