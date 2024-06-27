#!/usr/bin/env python
# coding: utf-8
#D.McKay Feb 2024

import sys
import os
from tqdm import tqdm
import warnings
import argparse
warnings.filterwarnings('ignore')

import bucket_manager.bucket_manager as bm

parser = argparse.ArgumentParser()
parser.add_argument('--bucket_name', '-b', type=str, help='The name of the S3 bucket.')
parser.add_argument('--download', action='store_true', default=False, help='Download the backup log.')
args = parser.parse_args()

bucket_name = args.bucket_name
download = args.download

try:
    keys = bm.get_keys('S3')
except KeyError as e:
    print(e)
    sys.exit()
s3_host = 'echo.stfc.ac.uk'
access_key = keys['access_key']
secret_key = keys['secret_key']

s3 = bm.get_resource(access_key,secret_key,s3_host)

bucket = s3.Bucket(bucket_name)
print('Bucket found.')

log_suffix = 'lsst-backup.csv'
previous_log_suffix = 'files.csv'

total_size = 0

# Download the backup log
# Limited to 1000 objects - this is to prevent this script from hanging if there are a large number of objects in the bucket
for ob in bucket.objects.filter(Prefix='butler').limit(1000):
    if ob.key.count('/') > 0:
        continue
    if log_suffix in ob.key or previous_log_suffix in ob.key:
        print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}')
        if download:
            with tqdm(total=ob.size/1024**2, unit='MiB', unit_scale=True, unit_divisor=1024) as pbar:
                bucket.download_file(ob.key,ob.key,Callback=pbar.update)
            print('Download complete.')