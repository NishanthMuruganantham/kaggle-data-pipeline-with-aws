#!/usr/bin/env python3
import aws_cdk as cdk
from parameters import (
    account_id,
    cricsheet_data_downloading_bucket_name,
    region,
    stack_name,
)
from mens_t20i_dataset_stack import MenT20IDatasetStack


app = cdk.App()
env = cdk.Environment(account=account_id, region=region)

MenT20IDatasetStack(
    app,
    stack_name,
    cricsheet_data_downloading_bucket_name=cricsheet_data_downloading_bucket_name,
    env=env,
    # If you don't specify 'env', this stack will be environment-agnostic.
    # Account/Region-dependent features and context lookups will not work,
    # but a single synthesized template can be deployed anywhere.

    # Uncomment the next line to specialize this stack for the AWS Account
    # and Region that are implied by the current CLI configuration.

    #env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')),

    # Uncomment the next line if you know exactly what Account and Region you
    # want to deploy the stack to. */

    #env=cdk.Environment(account='123456789012', region='us-east-1'),

    # For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
    )

app.synth()
