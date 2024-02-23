#!/usr/env/python

"""
Helper functions for using an s3 bucket
"""
import boto3
import json
import os

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

