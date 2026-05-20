import json
import boto3
import urllib.parse

# Initialize Step Functions client
sfn_client = boto3.client('stepfunctions')

# Replace with your Step Function ARN
STEP_FUNCTION_ARN = "arn:aws:states:ap-south-1:278029334708:stateMachine:netflix-elt-pipline"

def lambda_handler(event, context):
    try:
        # Extract file info from the S3 event
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        size = record['s3']['object'].get('size', 0)

        print(f"New file detected: s3://{bucket}/{key} (size={size})")

        # Prepare Step Function input
        input_data = {
            "s3_bucket": bucket,
            "s3_key": key,
            "file_size": size,
            "file_type": key.split('.')[-1],
        }

        # Start Step Function execution
        response = sfn_client.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            input=json.dumps(input_data)
        )

        print("Step Function started successfully.")
        print(f"Execution ARN: {response['executionArn']}")

        return {
            'statusCode': 200,
            'body': json.dumps('Step Function triggered successfully')
        }

    except Exception as e:
        print(f"Error triggering Step Function: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
