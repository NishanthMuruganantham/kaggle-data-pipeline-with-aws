#!/usr/bin/env python3
import yaml
import aws_cdk as cdk
from mens_t20i_dataset_stack import MenT20IDatasetStack


# Load configuration from YAML file
with open(r"settings.yaml", "r") as file:
    config = yaml.safe_load(file)


app = cdk.App()
env = cdk.Environment(account=config['account_id'], region=config['region'])

MenT20IDatasetStack(
    app,
    config["stack_name"],
    cricsheet_data_downloading_bucket_name=config["cricsheet_data_downloading_bucket"],
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
