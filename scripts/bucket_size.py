#!/usr/bin/env python
# coding: utf-8
# D. McKay Feb 2024

import sys
if len(sys.argv) != 2:
    sys.exit('Provide a bucket name as an argument.')
import os
import bucket_manager.bucket_manager as bm
from tqdm import tqdm

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
    print('Calculating bucket size...')
    sizes = tqdm([ob.size for ob in bucket.objects.all()])
except Exception as e:
    if '(NoSuchBucket)' in str(e).split():
        print('NoSuchBucket')

total_size = sum(sizes)

print(f'Total size of bucket: {total_size/1024**3:.2f} GiB')