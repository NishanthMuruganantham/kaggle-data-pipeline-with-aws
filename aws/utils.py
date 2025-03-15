"""
Utility functions for AWS services.
"""
import boto3
import json
from constants import SSM_PARAMETER_PREFIX


def get_parameter_from_ssm(ssm_client: boto3.client, parameter_name: str) -> str:
    """
    Fetches the parameter value from AWS SSM Parameter Store.

    :param ssm_client: SSM client
    :param parameter_name: Name of the parameter
    :return: Value of the parameter
    """
    try:
        response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
        return response['Parameter']['Value']
    except ssm_client.exceptions.ParameterNotFound:
        print(f"Parameter {parameter_name} not found.")
        return None


def get_secret_from_secrets_manager(secrets_manager_client: boto3.client, secret_key: str, secret_name: str) -> str:
    """
    Fetches the secret value from AWS Secrets Manager.

    :param secrets_manager_client: Secrets Manager client
    :param secret_key: Key of the secret
    :param secret_name: Name of the secret
    :return: Value of the secret    """
    try:
        response = secrets_manager_client.get_secret_value(SecretId=f"{SSM_PARAMETER_PREFIX}{secret_name}")
        response = json.loads(response["SecretString"])
        return response[secret_key]

    except secrets_manager_client.exceptions.ResourceNotFoundException:
        print(f"Secret {secret_name} not found.")
        return None

    except KeyError:
        print(f"Key {secret_key} not found in the secret.")
        return None
