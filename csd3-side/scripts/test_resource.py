import boto3
import os

access_key = os.environ['ECHO_ACCESS_KEY']
secret_key = os.environ['ECHO_SECRET_KEY']

s3_host = 'echo.stfc.ac.uk'

session = boto3.Session()

s3 = session.resource(
        service_name='s3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=f'https://{s3_host}',
        verify=False
    )
print(s3)