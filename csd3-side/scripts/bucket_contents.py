#!/usr/bin/env python
# coding: utf-8
# D. McKay Feb 2024

import sys
if len(sys.argv) != 2:
    sys.exit('Provide a bucket name as an argument.')
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import bucket_manager.bucket_manager as bm

s3_host = 'echo.stfc.ac.uk'
keys = bm.get_keys()
access_key = keys['access_key']
secret_key = keys['secret_key']

import warnings
warnings.filterwarnings('ignore')

s3 = bm.get_resource(access_key,secret_key,s3_host)

bucket_name = sys.argv[1]
bucket = s3.Bucket(bucket_name)
try:
    for ob in bucket.objects.all():
        print(ob.key)
except Exception as e:
    if '(NoSuchBucket)' in str(e).split():
        print('NoSuchBucket')

