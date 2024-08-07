import aws_cdk as core
import aws_cdk.assertions as assertions
from men_t20i_dataset.mens_t20i_dataset_stack import MenT20IDatasetStack


# example tests. To run these tests, uncomment this file along with the example
# resource in mens_t20i_dataset/mens_t20i_dataset_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = MenT20IDatasetStack(app, "mens-t20i-dataset")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
