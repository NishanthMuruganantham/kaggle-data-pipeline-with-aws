from aws_cdk import (
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
