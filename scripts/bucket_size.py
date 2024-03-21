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
#num_objects = sum(1 for _ in bucket.objects.all())
#print(num_objects)
total_size = 0
i=0
try:
    print('Calculating bucket size (".": 1000 objects)')
    # with tqdm(total=sum(1 for _ in bucket.objects.all())) as pbar:
    for ob in bucket.objects.all():
        total_size += ob.size
        if i%1000==0:
            print('.', end='', flush=True)
        i+=1
    print()
            # pbar.update(1)
    #sizes = [ob.size for ob in tqdm(bucket.objects.all(), total=sum(1 for _ in bucket.objects.all()))]
except Exception as e:
    if '(NoSuchBucket)' in str(e).split():
        print('NoSuchBucket')

# total_size = sum(sizes)

print(f'Total size of bucket: {i} objects; {total_size/1024**3:.2f} GiB')