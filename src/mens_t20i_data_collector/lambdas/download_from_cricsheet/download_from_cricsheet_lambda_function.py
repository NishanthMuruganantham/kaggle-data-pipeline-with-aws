from mens_t20i_data_collector.lambdas.download_from_cricsheet.lambda_logic import print_string


def handler(event, context):
    print(event)
    return {
        'statusCode': 200,
        'body': f'Hello from Lambda! {print_string}'
    }
