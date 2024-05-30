#!/usr/bin/env python
# coding: utf-8
# D. McKay Feb 2024

"""
This script allows you to delete a bucket in an S3-compatible storage service.
"""

import sys
import warnings
import argparse
# from tqdm import tqdm
import bucket_manager.bucket_manager as bm

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

parser = argparse.ArgumentParser()
parser.add_argument('-y', action='store_true', help='Skip confirmation prompt')
parser.add_argument('bucket_name', type=str, help='Name of the bucket to delete')
args = parser.parse_args()

bucket_name = args.bucket_name

if not bucket_name in bm.bucket_list(s3):
    sys.exit(f'Bucket "{bucket_name}" does not exist.')

if args.y:
    sure = 'y'
else:
    sure = input("Are you sure? [y/n]\n").lower()

if sure == 'n':
    sys.exit('Aborted.')
elif sure == 'y':
    print('Proceeding')
else:
    sys.exit('Aborted.')

bucket = s3.Bucket(bucket_name)

if len(list(bucket.objects.all())) > 0:
    response = bucket.objects.all().delete()
    
    try:
        deleted = [d['Key'] for d in response[0]['Deleted']]
        for d in deleted:
            print(f'Deleted object: {d}')
    except Exception as e:
        print(e)
    
    # Confirm
    if len(list(bucket.objects.all())) == 0:
        print(f'Bucket {bucket_name} emptied.')

try:
    bucket.delete()
except Exception as e:
    if '(BucketNotEmpty)' in str(e).split():
        print(f'Error: {bucket_name} BucketNotEmpty. Cannot delete.')
    else:
        print(e)

# Confirm
if not bucket_name in bm.bucket_list(s3):
    print(f'Bucket {bucket_name} deleted.')