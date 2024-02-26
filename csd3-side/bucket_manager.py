#!/usr/env/python

"""
Helper functions for using an s3 bucket
"""
import boto3
import json
import os, sys
import warnings
from botocore.exceptions import ClientError

def print_buckets(resource):
    for b in resource.buckets.all():
        print(b.name)
    
def get_keys(json_file):
	with open(json_file, 'r') as keyfile:
		keys = json.load(keyfile)
	return keys

def get_resource(access_key, secret_key, s3_host):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    return session.resource(
        service_name='s3',
        endpoint_url=f'https://{s3_host}',
        verify=False  # Disable SSL verification for non-AWS S3 endpoints
    )

def bucket_list(resource):
    return [ b.name for b in resource.buckets.all() ]

def create_bucket(resource):
    try:
        resource.create_bucket(Bucket=bucket_name)
    except Exception as e:
        print(e)
    return True

def print_objects(bucket):
    for obj in bucket.objects.all():
        print(obj.key)

def object_list(bucket):
    return [ obj.key for obj in bucket.objects.all() ]