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
parser.add_argument('--save-list', type=str, help='Write the list to file given absolute path.')
parser.add_argument('--limit', type=int, help='Limit the number of objects to list.', default=1000)
parser.add_argument('--log-csvs-only', action='store_true', default=True, help='List only the upload log CSV files.')
parser.add_argument('--verification-csvs-only', action='store_true', default=False, help='List only the upload verification CSV files.')
parser.add_argument('--all-csvs', action='store_true', default=False, help='List all backup-related CSV files. (Shortcut for --log-csvs-only --verification-csvs-only.)')
args = parser.parse_args()

bucket_name = args.bucket_name
download = args.download
limit = args.limit

if args.save_list:
    save_list = args.save_list
    if os.path.exists(save_list):
        print(f'{save_list} already exists. Exiting.')
        sys.exit()
    save_folder = os.path.dirname(save_list)
    if not os.path.exists(save_folder):
        print(f'{save_folder} does not exist. Exiting.')
        sys.exit()
else:
    save_list = None

if args.log_csvs_only:
    log_csvs_only = True

if args.verification_csvs_only:
    verification_csvs_only = True

if args.all_csvs:
    all_csvs = True

if not any([log_csvs_only, verification_csvs_only, all_csvs]):
    print('No list type specified. Listing log CSV files only.')
    log_csvs_only = True

if log_csvs_only and verification_csvs_only:
    all_csvs = True

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
verification_suffix = 'lsst-backup-verification.csv'

### When associating upload log csvs and verification csvs, use replace:
### verification_URI = upload_log_URI.replace(f'-{previous_log_suffix}',f'-{verification_suffix}')

total_size = 0

if all_csvs or log_csvs_only:
    log_csvs = []
if all_csvs or verification_csvs_only:
    verification_csvs = []

# Download the backup log
# Limited to 1000 objects by default - this is to prevent this script from hanging if there are a large number of objects in the bucket
for ob in bucket.objects.filter(Prefix='butler').limit(limit):
    if ob.key.count('/') > 0:
        continue
    if all_csvs or log_csvs_only:
        if log_suffix in ob.key or previous_log_suffix in ob.key:
            if save_list:
                with open(save_list,'a') as f:
                    f.write(f'{ob.key},{ob.size/1024**2:.2f},{ob.last_modified}\n')
            else:
                print(f'{ob.key},{ob.size/1024**2:.2f},{ob.last_modified}')
            log_csvs.append(ob)
    if all_csvs or verification_csvs_only:
        if verification_suffix in ob.key:
            if save_list:
                with open(save_list,'a') as f:
                    f.write(f'{ob.key},{ob.size/1024**2:.2f},{ob.last_modified}\n')
            else:
                print(f'{ob.key},{ob.size/1024**2:.2f},{ob.last_modified}')
            verification_csvs.append(ob)

if download:
    for key in log_csvs + verification_csvs:
        ob = bucket.Object(key)
        with tqdm(total=ob.size/1024**2, unit='MiB', unit_scale=True, unit_divisor=1024) as pbar:
            bucket.download_file(ob.key,ob.key,Callback=pbar.update)