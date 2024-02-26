import boto3
import json
import os, sys

def get_keys(json_file):
	with open(json_file, 'r') as keyfile:
		keys = json.load(keyfile)
	return keys

keys = get_keys(os.sep.join([os.environ['HOME'],'lsst_keys.json']))

access_key = keys['access_key']
secret_key = keys['secret_key']

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