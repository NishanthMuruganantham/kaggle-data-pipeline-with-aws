import boto3
from constants import SSM_PARAMETER_PREFIX
from utils import get_parameter_from_ssm


# Fetch values from SSM
ssm = boto3.client('ssm')
account_id = get_parameter_from_ssm(ssm, f"{SSM_PARAMETER_PREFIX}account_id")
cricsheet_data_downloading_bucket_name = get_parameter_from_ssm(ssm, f"{SSM_PARAMETER_PREFIX}cricsheet_data_downloading_bucket")
region = get_parameter_from_ssm(ssm, f"{SSM_PARAMETER_PREFIX}aws_region")
stack_name = get_parameter_from_ssm(ssm, f"{SSM_PARAMETER_PREFIX}stack_name")
DELIVERYWISE_DATA_COLLECTION_NAME = get_parameter_from_ssm(ssm, f"{SSM_PARAMETER_PREFIX}DELIVERYWISE_DATA_COLLECTION_NAME")
KAGGLE_USERNAME = get_parameter_from_ssm(ssm, f"{SSM_PARAMETER_PREFIX}KAGGLE_USERNAME")
KAGGLE_SECRET_KEY = get_parameter_from_ssm(ssm, f"{SSM_PARAMETER_PREFIX}KAGGLE_SECRET_KEY")
KAGGLE_DATASET_SLUG = get_parameter_from_ssm(ssm, f"{SSM_PARAMETER_PREFIX}KAGGLE_DATASET_SLUG")
MATCHWISE_DATA_COLLECTION_NAME = get_parameter_from_ssm(ssm, f"{SSM_PARAMETER_PREFIX}MATCHWISE_DATA_COLLECTION_NAME")
MONGO_DB_NAME = get_parameter_from_ssm(ssm, f"{SSM_PARAMETER_PREFIX}MONGO_DB_NAME")
MONGO_DB_URL = get_parameter_from_ssm(ssm, f"{SSM_PARAMETER_PREFIX}MONGO_DB_URL")
TELEGRAM_BOT_TOKEN = get_parameter_from_ssm(ssm, f"{SSM_PARAMETER_PREFIX}TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = get_parameter_from_ssm(ssm, f"{SSM_PARAMETER_PREFIX}TELEGRAM_CHAT_ID")
