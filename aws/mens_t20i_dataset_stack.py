from aws_cdk import (
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
    Duration,
    Stack,
    RemovalPolicy,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_s3_notifications as s3_notifications,
)
import boto3
from constructs import Construct
from constants import AWS_SDK_PANDAS_LAYER_ARN, THRESHOLD_FOR_NUMBER_OF_FILES_TO_BE_SENT_FOR_PROCESSING
from utils import get_secret_from_secrets_manager


class MenT20IDatasetStack(Stack):

    def __init__(
        self,
        scope: Construct,
        stack_name: str,
        cricsheet_data_downloading_bucket_name: str,
        **kwargs
    ) -> None:
        super().__init__(scope, stack_name, **kwargs)
        self._secret_manager_client = boto3.client("secretsmanager")

        # S3 bucket for downloading data from Cricsheet
        cricsheet_data_downloading_bucket = s3.Bucket(
            self,
            cricsheet_data_downloading_bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
            event_bridge_enabled=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        ######################################## DYNAMODB CONFIGURATIONS ################################################
        dynamodb_to_store_file_status_data = dynamodb.Table(
            self, f"{stack_name}-cricsheet_json_file_data_extraction_status_table",
            table_name=f"{stack_name}-cricsheet_json_file_data_extraction_status_table",
            partition_key=dynamodb.Attribute(
                name="file_name",
                type=dynamodb.AttributeType.STRING
            ),
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        ########################################  SECRET MANAGER Configurations ##########################################
        __db_secrets = get_secret_from_secrets_manager(self._secret_manager_client, "db_secret")
        __kaggle_secrets = get_secret_from_secrets_manager(self._secret_manager_client, "kaggle_credentials")

        ########################################  LAMBDA LAYER Configurations ##########################################

        # Lambda layer containing the necessary code and packages
        package_layer = _lambda.LayerVersion(
            self,
            f"{stack_name}-MensT20IDataCollectorLayer",
            code=_lambda.Code.from_asset("output/mens_t20i_data_collector.zip"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
            description="Layer containing the necessary code and packages for collecting men's T20I data",
        )
        # Pandas layer by AWS
        pandas_layer = _lambda.LayerVersion.from_layer_version_arn(self, "PandasLayer", AWS_SDK_PANDAS_LAYER_ARN)

        ########################################### LAMBDA CONFIGURATIONS #######################################################

        # Lambda function for downloading data from Cricsheet
        cricsheet_data_downloading_lambda = _lambda.Function(
            self,
            "cricsheet_data_downloading_lambda",
            code=_lambda.Code.from_asset("output/download_from_cricsheet_lambda_function.zip"),
            handler="download_from_cricsheet_lambda_function.handler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            environment={
                "DOWNLOAD_BUCKET_NAME": cricsheet_data_downloading_bucket.bucket_name,
                "DYNAMODB_TABLE_NAME": dynamodb_to_store_file_status_data.table_name,
                "THRESHOLD_FOR_NUMBER_OF_FILES_TO_BE_SENT_FOR_PROCESSING": THRESHOLD_FOR_NUMBER_OF_FILES_TO_BE_SENT_FOR_PROCESSING,
            },
            function_name="cricsheet-data-downloading-lambda",
            layers=[
                package_layer,
            ],
            timeout=Duration.minutes(10),
        )
        # Permissions for lambda functions to the S3 bucket
        cricsheet_data_downloading_bucket.grant_read_write(cricsheet_data_downloading_lambda)
        # Permissions for lambda functions to the DynamoDB table
        dynamodb_to_store_file_status_data.grant_read_write_data(cricsheet_data_downloading_lambda)
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
        # EventBridge Rule to trigger the Lambda every Monday at 12:00 AM UTC
        event_bridge_rule_to_trigger_cricsheet_data_downloading_lambda = events.Rule(
            self,
            "event_bridge_rule_to_trigger_cricsheet_data_downloading_lambda",
            schedule=events.Schedule.cron(
                minute="0",
                hour="0",
                month="*",
                week_day="MON",
                year="*",
            ),
            targets=[
                events_targets.LambdaFunction(cricsheet_data_downloading_lambda)
            ],
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
                **__db_secrets,
                "DYNAMODB_TABLE_NAME": dynamodb_to_store_file_status_data.table_name,
            },
            function_name="cricsheet-deliverywise-data-extraction-lambda",
            layers=[
                package_layer,
                pandas_layer,
            ],
            memory_size=300,
            timeout=Duration.minutes(1),
        )
        # Permissions for lambda functions to the S3 bucket
        cricsheet_data_downloading_bucket.grant_read_write(cricsheet_deliverywise_data_extraction_lambda)
        # Permissions for lambda functions to the DynamoDB table
        dynamodb_to_store_file_status_data.grant_read_write_data(cricsheet_deliverywise_data_extraction_lambda)
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
        event_bridge_rule_to_trigger_deliverywise_data_extraction_lambda = events.Rule(
            self,
            "event_bridge_rule_to_trigger_deliverywise_data_extraction_lambda",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {
                        "name": [cricsheet_data_downloading_bucket.bucket_name]
                    },
                    "object": {
                        "key": [{"prefix": "cricsheet_data/processed_data"}]
                    }
                },
            ),
        )
        event_bridge_rule_to_trigger_deliverywise_data_extraction_lambda.add_target(
            events_targets.LambdaFunction(cricsheet_deliverywise_data_extraction_lambda)
        )
        cricsheet_deliverywise_data_extraction_lambda.add_permission(
            "allow-s3-to-trigger-delivery-wise-data-extraction-lambda",
            principal=iam.ServicePrincipal("s3.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=event_bridge_rule_to_trigger_deliverywise_data_extraction_lambda.rule_arn,
        )

        # Lambda function for extracting matchwise cricsheet data
        cricsheet_matchwise_data_extraction_lambda = _lambda.Function(
            self,
            "cricsheet_matchwise_data_extraction_lambda",
            code=_lambda.Code.from_asset("output/extract_matchwise_cricsheet_data_lambda_function.zip"),
            handler="extract_matchwise_cricsheet_data_lambda_function.handler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            environment={
                "DOWNLOAD_BUCKET_NAME": cricsheet_data_downloading_bucket.bucket_name,
                **__db_secrets,
                "DYNAMODB_TABLE_NAME": dynamodb_to_store_file_status_data.table_name,
            },
            function_name="cricsheet-matchwise-data-extraction-lambda",
            layers=[
                package_layer,
                pandas_layer,
            ],
            memory_size=300,
            timeout=Duration.minutes(1),
        )
        # Permissions for lambda functions to the S3 bucket
        cricsheet_data_downloading_bucket.grant_read_write(cricsheet_matchwise_data_extraction_lambda)
        # Permissions for lambda functions to the DynamoDB table
        dynamodb_to_store_file_status_data.grant_read_write_data(cricsheet_matchwise_data_extraction_lambda)
        # Policy for CloudWatch logging
        cricsheet_matchwise_data_extraction_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )
        event_bridge_rule_to_trigger_matchwise_data_extraction_lambda = events.Rule(
            self,
            "event_bridge_rule_to_trigger_matchwise_data_extraction_lambda",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {
                        "name": [cricsheet_data_downloading_bucket.bucket_name]
                    },
                    "object": {
                        "key": [{"prefix": "cricsheet_data/processed_data"}]
                    }
                },
            ),
        )
        event_bridge_rule_to_trigger_matchwise_data_extraction_lambda.add_target(
            events_targets.LambdaFunction(cricsheet_matchwise_data_extraction_lambda)
        )
        cricsheet_matchwise_data_extraction_lambda.add_permission(
            "allow-s3-to-trigger-match-wise-data-extraction-lambda",
            principal=iam.ServicePrincipal("s3.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=event_bridge_rule_to_trigger_matchwise_data_extraction_lambda.rule_arn,
        )

        # Lambda function to convert the stored data in MongoDB table to CSV and store in S3
        convert_mongodb_data_to_csv_lambda = _lambda.Function(
            self,
            "convert_mongodb_data_to_csv_lambda",
            code=_lambda.Code.from_asset("output/convert_mongo_db_data_to_csv_lambda.zip"),
            handler="convert_mongo_db_data_to_csv_lambda.handler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            environment={
                "DOWNLOAD_BUCKET_NAME": cricsheet_data_downloading_bucket.bucket_name,
                **__db_secrets,
            },
            function_name="convert-mongo-data-to-csv-lambda",
            layers=[
                package_layer,
                pandas_layer,
            ],
            memory_size=3000,
            timeout=Duration.minutes(10),
        )
        # Permissions for lambda functions to the S3 bucket
        cricsheet_data_downloading_bucket.grant_read_write(convert_mongodb_data_to_csv_lambda)
        # Policy for CloudWatch logging
        convert_mongodb_data_to_csv_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )

        # Lambda function to upload the dataset to KAGGLE and create a new version of dataset
        upload_dataset_to_kaggle_lambda = _lambda.Function(
            self,
            "upload_dataset_to_kaggle_lambda",
            code=_lambda.Code.from_asset("output/upload_dataset_to_kaggle_lambda.zip"),
            handler="upload_dataset_to_kaggle_lambda.handler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            environment={
                "DOWNLOAD_BUCKET_NAME": cricsheet_data_downloading_bucket.bucket_name,
                **__kaggle_secrets,
            },
            function_name="upload-dataset-to-kaggle-lambda",
            layers=[
                package_layer,
                pandas_layer,
            ],
            memory_size=300,
            timeout=Duration.minutes(10),
        )
        # Permissions for lambda functions to the S3 bucket
        cricsheet_data_downloading_bucket.grant_read(upload_dataset_to_kaggle_lambda)
        # S3 bucket notification for the upload_dataset_to_kaggle_lambda
        cricsheet_data_downloading_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.LambdaDestination(upload_dataset_to_kaggle_lambda),
            s3.NotificationKeyFilter(prefix="output/", suffix="deliverywise_data.csv"),
        )
        # Policy for CloudWatch logging
        upload_dataset_to_kaggle_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )
