import os
import boto3

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    env = os.environ['ENV']
    bucket = os.environ['S3']
    prefix = f"{env}/files/"

    # List all objects in the folder
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    # Check if the folder is not empty
    if 'Contents' in response:
        # Iterate over the objects in the folder and delete them
        for item in response['Contents']:
            print(f"Deleting {item['Key']}...")
            s3.delete_object(Bucket=bucket, Key=item['Key'])
        return {"message": "All files deleted"}
    else:
        return {"message": "Folder is empty or does not exist"}
