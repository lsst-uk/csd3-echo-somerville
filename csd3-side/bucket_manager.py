#!/usr/env/python

"""
Helper functions for using an s3 bucket
"""
import boto3
import json
import os, sys
import warnings
from botocore.exceptions import ClientError

def print_buckets(client):
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
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

def bucket_list(client):
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        response = client.list_buckets()
    return [ b['Name'] for b in response['Buckets'] ]

def create_bucket(client,bucket_name):
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore')
            client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        print(f'ClientError {e}')
        return False
    except Error as e:
        print(f'Error {e}')
        return False
    return True

def object_list(client,bucket_name):
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        response = client.list_objects(Bucket=bucket_name)
    return [ c['Key'] for c in response['Contents'] ]
