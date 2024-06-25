#!/usr/bin/env python
# coding: utf-8
#D.McKay Feb 2024

# Check the checksums of files in an S3 bucket by verifying against the checksums in the upload log.
# The upload log is a CSV file with columns including 'FILE_SIZE', 'DESTINATION_KEY', and 'CHECKSUM'.
# The verification file is a CSV file with columns including 'DESTINATION_KEY', 'CHECKSUM', 'CHECKSUM_MATCH', 'NEW_CHECKSUM', 'FILE_SIZE', 'SIZE_ON_S3', and 'SIZE_MATCH'.
# The verification file is uploaded to the S3 bucket with the same name as the upload log but with '-verification' appended to the filename.
# Uses Dask for parallel processing.

from datetime import datetime
import sys
import os
from dask import dataframe as dd
import pandas as pd
from distributed import Client, wait, as_completed
from multiprocessing import cpu_count
import hashlib
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

import bucket_manager.bucket_manager as bm

def get_checksum(URI, access_key, secret_key, s3_host):
    """
    Calculate the MD5 checksum of an object stored in an S3 bucket.

    Parameters:
    URI (str): The URI of the object in the S3 bucket.
    access_key (str): The access key for the S3 bucket.
    secret_key (str): The secret key for the S3 bucket.
    s3_host (str): The hostname of the S3 service.

    Returns:
    str: The MD5 checksum of the object.

    """
    s3 = bm.get_resource(access_key, secret_key, s3_host)
    return hashlib.md5(s3.Object(bucket_name, URI).get()['Body'].read()).hexdigest()#.encode('utf-8')

def upload_verification_file(bucket_name, verification_path, verification_URI, access_key, secret_key, s3_host):
    """
    Uploads a verification file to the specified S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        verification_path (str): The local path of the verification file.
        verification_URI (str): The URI of the verification file in the S3 bucket.
        access_key (str): The access key for the S3 bucket.
        secret_key (str): The secret key for the S3 bucket.
        s3_host (str): The hostname of the S3 service.

    Returns:
        None
    """
    s3 = bm.get_resource(access_key, secret_key, s3_host)
    s3.meta.client.upload_file(verification_path, bucket_name, verification_URI)

if __name__ == '__main__':

    n_workers = cpu_count() - 2
    n_threads = 1

    start = datetime.now()

    print(f'Upload verification started at {start} || Elapsed time: 0:00:00.000000')

    client = Client(n_workers=n_workers,threads_per_worker=n_threads,memory_limit="2Gi")
    print(f'Dask client started with {n_workers} workers ({n_threads*n_workers} threads).')
    try:
        keys = bm.get_keys('S3')
    except KeyError as e:
        print(e)
        sys.exit()
    s3_host = 'echo.stfc.ac.uk'
    access_key = keys['access_key']
    secret_key = keys['secret_key']

    s3 = bm.get_resource(access_key,secret_key,s3_host)

    bucket_name = sys.argv[1]
    bucket = s3.Bucket(bucket_name)
    print('Bucket found.')
    upload_log_URI = sys.argv[2]

    log_suffix = 'lsst-backup.csv'
    previous_log_suffix = 'files.csv'
    verification_suffix = 'lsst-backup-verification.csv'

    if not upload_log_URI.endswith('.csv'):
        print('Upload log must be a .csv file.')
        sys.exit(1)
    upload_log_path = os.path.join(os.getcwd(),'upload_log.csv')
    verification_path = os.path.join(os.getcwd(),'verification.csv')

    #if statement only for backwards compatibility - future uploads will use log_suffix
    if upload_log_URI.endswith(previous_log_suffix):
        verification_URI = upload_log_URI.replace(f'-{previous_log_suffix}',f'-{verification_suffix}')
    else:
        verification_URI = upload_log_URI.replace(f'-{log_suffix}',f'-{verification_suffix}')
    try:
        print('Downloading upload log...')
        s3.meta.client.download_file(bucket_name, upload_log_URI, 'upload_log.csv')
    except Exception as e:
        if '(NoSuchBucket)' in str(e).split():
            print(f'NoSuchBucket {bucket_name}')
        elif '(NoSuchKey)' in str(e).split():
            print(f'NoSuchKey {upload_log_URI}')
    
    print(f'Upload log downloaded at {datetime.now()} || Elapsed time: {datetime.now()-start}.')

    # upload_log = pd.read_csv(upload_log_path)[['FILE_SIZE', 'DESTINATION_KEY', 'CHECKSUM']]
    # upload_log = upload_log[upload_log['DESTINATION_KEY'].str.endswith('.symlink') == False]

    batch_size = 1000
    upload_log = dd.read_csv(upload_log_path)[['FILE_SIZE', 'DESTINATION_KEY', 'CHECKSUM']]
    # not verifying symlinks 
    upload_log = upload_log[upload_log['DESTINATION_KEY'].str.endswith('.symlink') == False]
    
    #remove previously verified files from the upload log
    if os.path.exists(verification_path):
        verification = pd.read_csv(verification_path)
        upload_log = upload_log[~upload_log['DESTINATION_KEY'].isin(verification['DESTINATION_KEY'])]
    else:
        os.system(f'echo "DESTINATION_KEY,CHECKSUM,CHECKSUM_MATCH,NEW_CHECKSUM,FILE_SIZE,SIZE_ON_S3,SIZE_MATCH"> {verification_path}')

    #batch the upload log
    if len(upload_log) > batch_size:
        upload_log = upload_log.repartition(npartitions=len(upload_log) // batch_size)

    # Process each batch separately
    print(f'Verifying upload... {len(upload_log)} files to be verified using {len(upload_log) // batch_size} batches on {cpu_count()-2} cores.')

    for batch in upload_log.to_delayed():
        batch = batch.compute()  # Convert Dask DataFrame to Pandas DataFrame for this batch

        # Calculate checksums for this batch
        checksum_futures = [client.submit(get_checksum, object_key, access_key, secret_key, s3_host, retries=2) for object_key in batch['DESTINATION_KEY']]
        wait(checksum_futures)
        new_checksum = [future.result() for future in checksum_futures]

        # Compare checksums for this batch
        batch['NEW_CHECKSUM'] = new_checksum
        batch['CHECKSUM_MATCH'] = batch['CHECKSUM'] == batch['NEW_CHECKSUM']

        # Compare file sizes for this batch
        batch['SIZE_ON_S3'] = [s3.Object(bucket_name, URI).content_length for URI in batch['DESTINATION_KEY']]
        batch['SIZE_MATCH'] = batch['SIZE_ON_S3'] == batch['FILE_SIZE']

        # Append results for this batch to the verification file
        batch[['DESTINATION_KEY', 'CHECKSUM', 'CHECKSUM_MATCH', 'NEW_CHECKSUM', 'FILE_SIZE', 'SIZE_ON_S3', 'SIZE_MATCH']].to_csv(verification_path, mode='a', header=False, index=False)


    verification = dd.read_csv(verification_path)

    checksums_match = verification['CHECKSUM_MATCH'].all().compute()
    sizes_match = verification['SIZE_MATCH'].all().compute()

    if checksums_match and sizes_match:
        print('Upload successful.')
        print(f'Verification completed at: {datetime.now()} || Elapsed time: {datetime.now()-start}')
        upload_verification_file(bucket_name, verification_path, verification_URI, access_key, secret_key, s3_host)
        print(f'Verification file uploaded to s3://{s3_host}/{bucket_name}/{verification_URI}.')
        # Clean up
        os.remove(upload_log_path)
        os.remove(verification_path)
        sys.exit(0)
    else:
        print('Upload failed.')
        # Clean up
        os.remove(upload_log_path)
        sys.exit(1)
