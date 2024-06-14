#!/usr/bin/env python
# coding: utf-8
# D. McKay Feb 2024

import sys
if len(sys.argv) != 2:
    sys.exit('Provide a bucket name as an argument.')
import os
import bucket_manager.bucket_manager as bm

s3_host = 'echo.stfc.ac.uk'
try:
    keys = bm.get_keys()
except KeyError as e:
    print(e)
    sys.exit()
access_key = keys['access_key']
secret_key = keys['secret_key']

import warnings
warnings.filterwarnings('ignore')

s3 = bm.get_resource(access_key,secret_key,s3_host)

bucket_name = sys.argv[1]
bucket = s3.Bucket(bucket_name)
total_size = 0
try:
    print('Contents of bucket: ',bucket_name)
    print('Object, Size / MiB, LastModified, Zip Contents')
    for ob in bucket.objects.all():
        if ob.key.endswith('zip'):
            try:
                print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, "{s3.ObjectSummary(bucket_name, ob.key).get()["Metadata"]["zip-contents"]}"')
            except Exception as e:
                print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, n/a')
        else:
            print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, n/a')
        total_size += ob.size
except Exception as e:
    if '(NoSuchBucket)' in str(e).split():
        print('NoSuchBucket')

print(f'Total size of bucket: {total_size/1024**3:.2f} GiB')