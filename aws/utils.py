"""
Utility functions for AWS services.
"""
import boto3


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
