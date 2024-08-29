from aws_cdk import (
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_notifications as s3_notification,
    Duration,
    Stack,
    RemovalPolicy,
)
from constructs import Construct
from constants import AWS_SDK_PANDAS_LAYER_ARN


class MenT20IDatasetStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        cricsheet_data_downloading_bucket_name: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 bucket for downloading data from Cricsheet
        cricsheet_data_downloading_bucket = s3.Bucket(
            self,
            cricsheet_data_downloading_bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Lambda layer containing the necessary code and packages
        package_layer = _lambda.LayerVersion(
            self,
            "MensT20IDataCollectorLayer",
            code=_lambda.Code.from_asset("output/mens_t20i_data_collector.zip"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
            description="Layer containing the necessary code and packages for collecting men's T20I data",
        )
        # Pandas layer by AWS
        pandas_layer = _lambda.LayerVersion.from_layer_version_arn(self, "PandasLayer", AWS_SDK_PANDAS_LAYER_ARN)

        # Lambda function for downloading data from Cricsheet
        cricsheet_data_downloading_lambda = _lambda.Function(
            self,
            "cricsheet_data_downloading_lambda",
            code=_lambda.Code.from_asset("output/download_from_cricsheet_lambda_function.zip"),
            handler="download_from_cricsheet_lambda_function.handler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            environment={
                "DOWNLOAD_BUCKET_NAME": cricsheet_data_downloading_bucket.bucket_name,
            },
            function_name="cricsheet-data-downloading-lambda",
            layers=[
                package_layer,
            ],
            timeout=Duration.minutes(1),
        )
        # Permissions for lambda functions to the S3 bucket
        cricsheet_data_downloading_bucket.grant_read_write(cricsheet_data_downloading_lambda)
        # Policy for CloudWatch logging
        cricsheet_data_downloading_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )

        # Lambda function for extracting deliverywise cricsheet data
        cricsheet_deliverywise_data_extraction_lambda = _lambda.Function(
            self,
            "cricsheet_deliverywise_data_extraction_lambda",
            code=_lambda.Code.from_asset("output/extract_deliverywise_cricsheet_data_lambda_function.zip"),
            handler="extract_deliverywise_cricsheet_data_lambda_function.handler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            environment={
                "DOWNLOAD_BUCKET_NAME": cricsheet_data_downloading_bucket.bucket_name,
            },
            function_name="cricsheet-deliverywise-data-extraction-lambda",
            layers=[
                package_layer,
                pandas_layer,
            ],
            memory_size=300,
            timeout=Duration.minutes(10),
        )
        # Permissions for lambda functions to the S3 bucket
        cricsheet_data_downloading_bucket.grant_read_write(cricsheet_deliverywise_data_extraction_lambda)
        # Policy for CloudWatch logging
        cricsheet_deliverywise_data_extraction_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )

        # S3 event notification to trigger the processing lambda
        cricsheet_data_downloading_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notification.LambdaDestination(cricsheet_deliverywise_data_extraction_lambda),
            s3.NotificationKeyFilter(prefix="cricsheet_data/new_cricsheet_data/", suffix=".zip")
        )
