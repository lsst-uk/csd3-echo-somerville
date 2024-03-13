#!/usr/bin/env python
# coding: utf-8
#D.McKay Feb 2024
# Check the checksums of files in an S3 bucket by verifying against the checksums in the upload log.

from datetime import datetime
import sys
import os
from dask import dataframe as dd
import pandas as pd
from distributed import Client, wait #, progress
import hashlib
from tqdm import tqdm
import numpy as np
import warnings
warnings.filterwarnings('ignore')

import bucket_manager.bucket_manager as bm

def get_checksum(URI, access_key, secret_key, s3_host):
    s3 = bm.get_resource(access_key,secret_key,s3_host)
    return hashlib.md5(s3.Object(bucket_name, URI).get()['Body'].read()).hexdigest()#.encode('utf-8')

import hashlib

def get_checksum_a(args):
    """
    Calculate the MD5 checksum of an object in an S3 bucket.

    Args:
        args (tuple): A tuple containing the URI, access key, secret key, and S3 host.

    Returns:
        str: The MD5 checksum of the object.

    """
    URI, access_key, secret_key, s3_host = args
    s3 = bm.get_resource(access_key, secret_key, s3_host)
    return hashlib.md5(s3.Object(bucket_name, URI).get()['Body'].read()).hexdigest()#.encode('utf-8')

if __name__ == '__main__':

    client = Client(n_workers=8,threads_per_worker=1,memory_limit="1Gi")

    keys = bm.get_keys('S3')
    s3_host = 'echo.stfc.ac.uk'
    access_key = keys['access_key']
    secret_key = keys['secret_key']

    s3 = bm.get_resource(access_key,secret_key,s3_host)

    bucket_name = sys.argv[1]
    bucket = s3.Bucket(bucket_name)

    upload_log_URI = sys.argv[2]
    upload_log_path = os.path.join(os.getcwd(),'upload_log.csv')

    try:
        s3.meta.client.download_file(bucket_name, upload_log_URI, 'upload_log.csv')
    except Exception as e:
        if '(NoSuchBucket)' in str(e).split():
            print(f'NoSuchBucket {bucket_name}')
        elif '(NoSuchKey)' in str(e).split():
            print(f'NoSuchKey {upload_log_URI}')

    # upload_log = pd.read_csv(upload_log_path)[['FILE_SIZE', 'DESTINATION_KEY', 'CHECKSUM']]
    # upload_log = upload_log[upload_log['DESTINATION_KEY'].str.endswith('.symlink') == False]

    upload_log = dd.read_csv(upload_log_path)[['FILE_SIZE', 'DESTINATION_KEY', 'CHECKSUM']]
    # upload_log.
    upload_log = upload_log[upload_log['DESTINATION_KEY'].str.endswith('.symlink') == False]
    upload_log = upload_log.compute()[:1]

    # print(upload_log)

    # Get objects "and checksum them"
    start = datetime.now()
    print('dask')
    checksum_futures = [client.submit(get_checksum, object_key, access_key, secret_key, s3_host, retries=2) for object_key in upload_log['DESTINATION_KEY']]
    for _ in tqdm(as_completed(checksum_futures), total=len(upload_log)):
        pass
    
    new_checksum = [future.result() for future in checksum_futures]
    print(new_checksum)
    print(f'Dask timing: {datetime.now()-start}')

    # close Dask client
    client.close()

    # match booleans
    checksums_match = None
    sizes_match = None
    # Compare checksums
    upload_log['NEW_CHECKSUM'] = new_checksum
    upload_log['CHECKSUM_MATCH'] = upload_log['CHECKSUM'] == upload_log['NEW_CHECKSUM']
    
    if upload_log['CHECKSUM_MATCH'].all():
        checksums_match = True
        print('Checksums match.')
    else:    
        checksums_match = False
        print('Checksums do not match.')
    # Compare file sizes
    upload_log['SIZE_ON_S3'] = [s3.Object(bucket_name, URI).content_length for URI in tqdm(upload_log['DESTINATION_KEY'])]
    upload_log['SIZE_MATCH'] = upload_log['SIZE_ON_S3'] == upload_log['FILE_SIZE']
    if upload_log['SIZE_MATCH'].all():
        sizes_match = True
        print('Sizes match.')
    else:
        sizes_match = False
        print('Sizes do not match.')

    if checksums_match and sizes_match:
        print('Upload successful.')
        # Clean up
        os.remove(upload_log_path)
        sys.exit(0)
    else:
        print('Upload failed.')
        # Clean up
        os.remove(upload_log_path)
        sys.exit(1)
