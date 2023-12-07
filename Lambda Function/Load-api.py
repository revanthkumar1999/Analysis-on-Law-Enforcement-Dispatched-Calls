import os
import boto3
import requests
from datetime import datetime

def lambda_handler(event, context):
    # The URL for the CSV dataset
    csv_url = 'https://data.sfgov.org/api/views/gnap-fj3t/rows.csv?accessType=DOWNLOAD'
    
    # Send a GET request to the URL
    response = requests.get(csv_url)
    if response.status_code != 200:
        # If the response was not successful, return an error
        return {
            'statusCode': response.status_code,
            'body': 'Failed to retrieve CSV data'
        }

    csv_data = response.content
    
    env = os.environ['ENV']
    s3_bucket = os.environ['S3'] 
    
    s3_key = f"{env}/files/le_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"  # The S3 object key

    # Initialize an S3 client using boto3
    s3 = boto3.client('s3', region_name='us-west-2')
    
    
    # Upload the CSV data to the specified S3 bucket
    try:
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_data)
        return {
            'statusCode': 200,
            'body': f'Successfully uploaded CSV to s3://{s3_bucket}/{s3_key}'
        }
    except Exception as e:
        # If there was any error during upload, return an error
        return {
            'statusCode': 500,
            'body': f'Error saving CSV to S3: {str(e)}'
        }

