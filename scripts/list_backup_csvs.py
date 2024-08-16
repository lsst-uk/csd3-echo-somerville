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
parser.add_argument('--bucket_name', '-b', type=str, help='The name of the S3 bucket.', required=True)
parser.add_argument('--download', '-d', type=str, default=None, help='Download the backup CSV files to given absolute path to a download directory. Must not already exist.')
parser.add_argument('--save-list', '-s', type=str, help='Write the list to file given absolute path.')
parser.add_argument('--limit', type=int, help='Limit the number of objects to list.', default=1000)
parser.add_argument('--log-csvs', '-L', action='store_true', default=False, help='List only the upload log CSV files.')
parser.add_argument('--verification-csvs', '-V', action='store_true', default=False, help='List only the upload verification CSV files.')
parser.add_argument('--all-csvs', '-A', action='store_true', default=True, help='List all backup-related CSV files. (Shortcut for --log-csvs --verification-csvs.)')
args = parser.parse_args()

bucket_name = args.bucket_name
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

download_dir = None

if args.download is not None:
    download_dir = args.download
    if os.path.exists(download_dir):
        print(f'{download_dir} already exists. Exiting.')
        sys.exit()
    os.makedirs(download_dir)

verification_csvs = False
log_csvs = False

if args.log_csvs:
    log_csvs = True
    
if args.verification_csvs:
    verification_csvs = True

if args.all_csvs:
    log_csvs = True
    verification_csvs = True

if not any([log_csvs, verification_csvs]):
    print('No list type specified. Listing log CSV files only.')
    log_csvs = True

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

total_size = 0

log_csvs_list = []
verification_csvs_list = []

# Download the backup log
# Limited to 1000 objects by default - this is to prevent this script from hanging if there are a large number of objects in the bucket
print('Backup CSV\t\t\t\tLast Modified')
for ob in bucket.objects.limit(limit):
    if ob.key.count('/') > 0:
        continue
    if log_csvs:
        if log_suffix in ob.key or previous_log_suffix in ob.key:
            print(f'{ob.key}\t{ob.last_modified}')
            log_csvs_list.append(ob.key)
    if verification_csvs:
        if verification_suffix in ob.key:
            print(f'{ob.key}\t{ob.last_modified}')
            verification_csvs_list.append(ob.key)

if save_list:
    print(f'List saved to {save_list}.')
    with open(save_list,'a') as f:
        for key in log_csvs_list + verification_csvs_list:
            f.write(f'{key}\n')

if download_dir:
    print(f'Downloading to {download_dir}...') 
    for key in log_csvs_list + verification_csvs_list:
        ob = bucket.Object(key)
        with tqdm(total=ob.content_length/1024**2, unit='MiB', unit_scale=True, unit_divisor=1024) as pbar:
            ob.download_file('/'.join([download_dir,ob.key]),Callback=pbar.update)

print('Done.')