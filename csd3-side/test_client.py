import boto3
import json
import os, sys
from botocore.exceptions import ClientError

from time import sleep

def print_buckets(client):
	response = client.list_buckets()
	for bucket in response['Buckets']:
        	print(f"{bucket['Name']}\t{bucket['CreationDate']}")

def get_keys(json_file):
	with open(json_file, 'r') as keyfile:
		keys = json.load(keyfile)
	return keys

def get_client(access_key, secret_key, host):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    return session.client(
        service_name='s3',
        endpoint_url=f'https://{host}',
        verify=False  # Disable SSL verification for non-AWS S3 endpoints
    )

keys = get_keys(os.sep.join([os.environ['HOME'],'lsst_keys.json']))

access_key = keys['access_key']
secret_key = keys['secret_key']

s3_host = 'echo.stfc.ac.uk'

try:
	client = get_client(access_key, secret_key, s3_host)
except ClientError as e:
	sys.exit(f'boto3 ClientError: {e}')
print(client)
