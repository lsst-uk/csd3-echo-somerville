#!/usr/bin/env python
# coding: utf-8
# D. McKay Feb 2024

"""
This script allows you to upload a single file given bucket name, object_name and local path.
"""

import sys
import warnings
import argparse

import bucket_manager.bucket_manager as bm



parser = argparse.ArgumentParser(description='Upload a file to an S3 bucket.')
parser.add_argument('--bucket-name', '-b', type=str, help='The name of the S3 bucket.')
parser.add_argument('--object-name', '-o', type=str, help='The name of the object to upload.')
parser.add_argument('--local-path', '-p', type=str, help='The local path to the file to upload.')

args = parser.parse_args()

bucket_name = args.bucket_name
object_name = args.object_name
local_path = args.local_path

s3_host = 'echo.stfc.ac.uk'
try:
    keys = bm.get_keys()
except KeyError as e:
    print(e)
    sys.exit()
access_key = keys['access_key']
secret_key = keys['secret_key']

warnings.filterwarnings('ignore')

s3 = bm.get_resource(access_key, secret_key, s3_host)

if not bucket_name in bm.bucket_list(s3):
    sys.exit('Bucket does not exist.')

bucket = s3.Bucket(bucket_name)

# if not object_name in bm.object_list(bucket):
#     sys.exit('Object does not exist.')


response = bucket.upload_file(local_path, object_name) 
if response:
    print(response)
else:
    print('Upload successful.')