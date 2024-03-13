#!/usr/bin/env python
# coding: utf-8
# D. McKay Feb 2024

"""
This script allows you to delete a bucket in an S3-compatible storage service.
"""

import sys
import os
import warnings

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import bucket_manager.bucket_manager as bm

s3_host = 'echo.stfc.ac.uk'
keys = bm.get_keys(api='S3')
access_key = keys['access_key']
secret_key = keys['secret_key']

warnings.filterwarnings('ignore')

s3 = bm.get_resource(access_key, secret_key, s3_host)

bucket_name = sys.argv[1]

if not bucket_name in bm.bucket_list(s3):
    sys.exit('Bucket does not exist.')

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