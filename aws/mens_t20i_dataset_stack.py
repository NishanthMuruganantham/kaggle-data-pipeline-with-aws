from aws_cdk import (
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscription,
    aws_sqs as sqs,
    aws_s3 as s3,
    Duration,
    Stack,
    RemovalPolicy,
    aws_lambda_event_sources
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

        ######################################## DYNAMODB CONFIGURATIONS ################################################
        dynamo_db_for_storing_deliverywise_data = dynamodb.Table(
            self,
            "dynamo_db_for_storing_deliverywise_data",
            table_name="dynamo_db_for_storing_deliverywise_data",
            partition_key=dynamodb.Attribute(
                name="composite_delivery_key",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="match_id",
                type=dynamodb.AttributeType.NUMBER,
            ),
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )
        dynamo_db_for_storing_matchwise_data = dynamodb.Table(
            self,
            "dynamo_db_for_storing_matchwise_data",
            table_name="dynamo_db_for_storing_matchwise_data",
            partition_key=dynamodb.Attribute(
                name="index",
                type=dynamodb.AttributeType.NUMBER,
            ),
            sort_key=dynamodb.Attribute(
                name="match_id",
                type=dynamodb.AttributeType.NUMBER,
            ),
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        ########################################  SNS Configurations #####################################################
        # SNS Topic from which the SQS queues get the data
        cricsheet_json_data_extraction_sns_topic = sns.Topic(
            self,
            "cricsheet_json_data_extraction_sns_topic",
            topic_name="cricsheet_json_data_extraction_sns_topic",
        )

        ######################################## DLQ Configurations ######################################################
        # DLQ for the deliverywise data extraction from the JSON files
        cricsheet_deliverywise_data_extraction_dlq = sqs.Queue(
            self,
            "cricsheet_deliverywise_data_extraction_dlq",
            queue_name="cricsheet_deliverywise_data_extraction_dlq",
            retention_period=Duration.days(14),
        )
        # DLQ for the matchwise data extraction from the JSON files
        cricsheet_matchwise_data_extraction_dlq = sqs.Queue(
            self,
            "cricsheet_matchwise_data_extraction_dlq",
            queue_name="cricsheet_matchwise_data_extraction_dlq",
            retention_period=Duration.days(14),
        )

        ########################################### SQS Configurations ###################################################
        # SQS Topic for the deliverywise data extraction from the JSON files
        cricsheet_deliverywise_data_extraction_sqs_queue = sqs.Queue(
            self,
            "cricsheet_deliverywise_data_extraction_sqs_queue",
            queue_name="cricsheet_deliverywise_data_extraction_sqs_queue",
            visibility_timeout=Duration.minutes(10),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=2,
                queue=cricsheet_deliverywise_data_extraction_dlq,
            ),
        )
        cricsheet_json_data_extraction_sns_topic.add_subscription(
            sns_subscription.SqsSubscription(cricsheet_deliverywise_data_extraction_sqs_queue)
        )
        # SQS Topic for the matchwise data extraction from the JSON files
        cricsheet_matchwise_data_extraction_sqs_queue = sqs.Queue(
            self,
            "cricsheet_matchwise_data_extraction_sqs_queue",
            queue_name="cricsheet_matchwise_data_extraction_sqs_queue",
            visibility_timeout=Duration.minutes(10),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=2,
                queue=cricsheet_matchwise_data_extraction_dlq,
            ),
        )
        cricsheet_json_data_extraction_sns_topic.add_subscription(
            sns_subscription.SqsSubscription(cricsheet_matchwise_data_extraction_sqs_queue)
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
                "SNS_TOPIC_ARN": cricsheet_json_data_extraction_sns_topic.topic_arn,
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
        cricsheet_data_downloading_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["sns:Publish"],
                resources=[cricsheet_json_data_extraction_sns_topic.topic_arn],
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
        # Permissions for lambda functions to the DynamoDB table
        dynamo_db_for_storing_deliverywise_data.grant_read_write_data(cricsheet_deliverywise_data_extraction_lambda)
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
        cricsheet_deliverywise_data_extraction_lambda.add_event_source(
            aws_lambda_event_sources.SqsEventSource(cricsheet_deliverywise_data_extraction_sqs_queue)
        )

        # Lambda function for extracting matchwise cricsheet data
        cricsheet_matchwise_data_extraction_lambda = _lambda.Function(
            self,
            "cricsheet_matchwise_data_extraction_lambda",
            code=_lambda.Code.from_asset("output/extract_matchwise_cricsheet_data_lambda_function.zip"),
            handler="extract_matchwise_cricsheet_data_lambda_function.handler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            environment={
                "DYNAMODB_TO_STORE_MATCHWISE_DATA": dynamo_db_for_storing_matchwise_data.table_name,
                "DOWNLOAD_BUCKET_NAME": cricsheet_data_downloading_bucket.bucket_name,
            },
            function_name="cricsheet-matchwise-data-extraction-lambda",
            layers=[
                package_layer,
                pandas_layer,
            ],
            memory_size=300,
            timeout=Duration.minutes(10),
        )
        # Permissions for lambda functions to the S3 bucket
        cricsheet_data_downloading_bucket.grant_read_write(cricsheet_matchwise_data_extraction_lambda)
        # Permissions for lambda functions to the DynamoDB table
        dynamo_db_for_storing_matchwise_data.grant_read_write_data(cricsheet_matchwise_data_extraction_lambda)
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
        cricsheet_matchwise_data_extraction_lambda.add_event_source(
            aws_lambda_event_sources.SqsEventSource(cricsheet_matchwise_data_extraction_sqs_queue)
        )

        # Lambda function to convert the stored data in DynamoDB table to CSV and store in S3
        convert_dynamodb_data_to_csv_lambda = _lambda.Function(
            self,
            "convert_dynamodb_data_to_csv_lambda",
            code=_lambda.Code.from_asset("output/convert_dynamo_db_data_to_csv_lambda.zip"),
            handler="convert_dynamo_db_data_to_csv_lambda.handler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            environment={
                "DYNAMODB_TO_STORE_DELIVERYWISE_DATA": dynamo_db_for_storing_deliverywise_data.table_name,
                "DOWNLOAD_BUCKET_NAME": cricsheet_data_downloading_bucket.bucket_name,
            },
            function_name="convert-dynamodb-data-to-csv-lambda",
            layers=[
                package_layer,
                pandas_layer,
            ],
            memory_size=300,
            timeout=Duration.minutes(10),
        )
        # Permissions for lambda functions to the S3 bucket
        cricsheet_data_downloading_bucket.grant_read_write(convert_dynamodb_data_to_csv_lambda)
        # Permissions for lambda functions to the DynamoDB table
        dynamo_db_for_storing_deliverywise_data.grant_read_data(convert_dynamodb_data_to_csv_lambda)
        # Policy for CloudWatch logging
        convert_dynamodb_data_to_csv_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )
