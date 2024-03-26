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

    client = Client(n_workers=cpu_count()-2,threads_per_worker=1,memory_limit="2Gi")

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

    # print(upload_log)

    # Get objects "and checksum them"
    start = datetime.now()
    print('dask')
    checksum_futures = [client.submit(get_checksum, object_key, access_key, secret_key, s3_host, retries=2) for object_key in upload_log['DESTINATION_KEY']]
    for _ in tqdm(as_completed(checksum_futures), total=len(upload_log)):
        pass
    wait(checksum_futures)
    new_checksum = [future.result() for future in checksum_futures]
#    print(new_checksum)
    print(f'Dask timing: {datetime.now()-start}')

    # close Dask client
    client.close()

    # match booleans
    checksums_match = None
    sizes_match = None
    # Compare checksums
    upload_log = upload_log.compute()
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
        verification = upload_log[['DESTINATION_KEY', 'CHECKSUM', 'CHECKSUM_MATCH', 'NEW_CHECKSUM', 'FILE_SIZE', 'SIZE_ON_S3', 'SIZE_MATCH']]
        verification.to_csv(verification_path, index=False)
        upload_verification_file(bucket_name, verification_path, verification_URI, access_key, secret_key, s3_host)
        print('Verification file uploaded.')
        # Clean up
        os.remove(upload_log_path)
        os.remove(verification_path)
        sys.exit(0)
    else:
        print('Upload failed.')
        # Clean up
        os.remove(upload_log_path)
        sys.exit(1)
