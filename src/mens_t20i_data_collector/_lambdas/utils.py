import logging
import os
from typing import Any

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_environmental_variable_value(variable_name: str) -> Any:
    value = os.getenv(variable_name)
    if value is None:
        logger.error(f"Environmental variable {variable_name} is not set")
        raise ValueError(f"Environmental variable {variable_name} is not set")
    return value
