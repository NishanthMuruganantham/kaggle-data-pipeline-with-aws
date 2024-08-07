from aws_cdk import (
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
    Stack,
    RemovalPolicy,
)
from constructs import Construct


class MenT20IDatasetStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        cricsheet_data_downloading_bucket_name: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Creating S3 Bucket for downloading data from cricsheet
        cricsheet_data_downloading_bucket = s3.Bucket(
            self, cricsheet_data_downloading_bucket_name, removal_policy=RemovalPolicy.DESTROY,
        )

        # Defining the lambda function
        cricsheet_data_downloading_lambda = _lambda.Function(
            self,
            "cricsheet_data_downloading_lambda",
            code=_lambda.Code.from_asset("src/lambdas/download_from_cricsheet"),
            environment={
                "DOWNLOAD_BUCKET_NAME": cricsheet_data_downloading_bucket.bucket_name,
            },
            function_name="cricsheet-data-downloading-lambda",
            handler="download_from_cricsheet_lambda_function.handler",
            runtime=_lambda.Runtime.PYTHON_3_9,
        )

        # Granting the lambda function access to the S3 bucket
        cricsheet_data_downloading_bucket.grant_read_write(cricsheet_data_downloading_lambda)

        cricsheet_data_downloading_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                resources=["*"]
            )
        )
