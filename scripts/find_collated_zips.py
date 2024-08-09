#!/usr/bin/env python
# coding: utf-8
#D.McKay Jun 2024


import sys
import os
from multiprocessing import Pool
from itertools import repeat
import warnings
from datetime import datetime, timedelta
from time import sleep
import hashlib
import base64
import pandas as pd
import numpy as np
import glob
import subprocess
import yaml
import io
import zipfile
import warnings
warnings.filterwarnings('ignore')

import bucket_manager.bucket_manager as bm

import hashlib
import os
import argparse

import re

def get_key_lists(bucket_name, access_key, secret_key, s3_host, get_contents_metadata, debug):
    zipfile_list = []
    contents_list = []
    all_keys_list = []
    s3 = bm.get_resource(access_key, secret_key, s3_host)
    s3_client = bm.get_client(access_key, secret_key, s3_host)
    bucket = s3.Bucket(bucket_name)

    pattern = re.compile(r'.*collated_\d+\.zip$')

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name)
    key_count = 0
    zipfile_count = 0
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                key_count += 1
                key = obj['Key']
                if pattern.match(key):
                    zipfile_count += 1
                    zipfile_list.append(key)
                    if get_contents_metadata:
                        contents = bucket.Object(key).get()['Metadata']['zip-contents'].split(',')
                        contents_list.append(contents)
                        # print(f'{key}: {contents}')
                    # else:
                else:
                    all_keys_list.append(key)
            print(f'Keys found: {key_count}, Zip files found: {zipfile_count}', end='\r')
            # for debugging
            if debug:
                if key_count >= 1000:
                    break
    print()
    zipfile_df = pd.DataFrame(np.array([zipfile_list,contents_list], dtype=object).T, columns=['zipfile','contents'])
    if debug:
        print(zipfile_df)
    return zipfile_df, pd.Series(all_keys_list, name='all_keys')

def verify_zip_contents(zipfiles_df, all_keys, debug):
    """
    Verify the contents of a zipfile against a list of keys.

    Parameters:
    - zipfile_df (pandas.DataFrame): The dataframe containing the zipfile information.
    - all_keys_s (pandas.Series): The series containing all the keys to check against.
    - debug (bool): Flag indicating whether to print debug information.

    Returns:
    None
    """
    extract_list = []
    # print(zipfiles_df)
    # print(all_keys)
    print('Checking for zipfile contents in all_keys list...')
    start = datetime.now()
    for i in range(len(zipfiles_df)):
        # for key in zipfiles_df['contents'].iloc[i]:
        #     print(key)
        #     print(all_keys.isin([key]).any())
        if sum(all_keys.isin(zipfiles_df['contents'].iloc[i])) != len(zipfiles_df['contents'].iloc[i]):
            extract_list.append(zipfiles_df.iloc[i]["zipfile"])
        else:
            print(f'{zipfiles_df.iloc[i]["zipfile"]} contents previously extracted.')
    print((datetime.now()-start).microseconds, 'microseconds')
    return extract_list

def prepend_zipfile_path_to_contents(zipfile_df, debug):
    """
    Prepend the path to the zipfile to the contents column in the given DataFrame.
    Contents by default are relative to the zipfile location.
    Parameters:
    - zipfile_df (DataFrame): The DataFrame containing the zipfile information.
    - debug (bool): A flag indicating whether to print debug information.
    Returns:
    - None
    """
    zipfile_df['path_stubs'] = ['/'.join(x.split('/')[:-1]) for x in zipfile_df['zipfile']]
    zipfile_df['contents'] = [[f'{zipfile_df.iloc[i]["path_stubs"]}/{x}' for x in zipfile_df.iloc[i]['contents']] for i in range(len(zipfile_df))]
    return zipfile_df.drop(columns='path_stubs')

def extract_and_upload_zipfiles(extract_list, bucket_name, access_key, secret_key, s3_host, debug):
    s3 = bm.get_resource(access_key, secret_key, s3_host)
    s3_client = bm.get_client(access_key, secret_key, s3_host)
    bucket = s3.Bucket(bucket_name)
    
    for zipfile_key in extract_list:
        print(f'Extracting {zipfile_key}...')
        path_stub = '/'.join(zipfile_key.split('/')[:-1])
        zipfile_data = io.BytesIO(bucket.Object(zipfile_key).get()['Body'].read())
        with zipfile.ZipFile(zipfile_data) as zf:
            for content_file in zf.namelist():
                print(content_file)
                content_file_data = zf.open(content_file)
                key = path_stub + '/' + content_file
                bucket.upload_fileobj(content_file_data, f'{key}')
                print(f'Uploaded {content_file} to {key}')
        if debug: # stop after frist zipfile
            exit()


def main():
    """
    Search for zip files created by lsst-backup.py in a given S3 bucket on echo.stfc.ac.uk.
    Args:
        --bucket-name, -b (str): Name of the S3 bucket. (required)
        --list-contents, -l (bool): List the contents of the zip files.
        --verify-contents, -v (bool): Verify the contents of the zip files from metadata.
        --debug, -d (bool): Print debug messages and shorten search.
    Returns:
        None
    """
    epilog = ''
    class MyParser(argparse.ArgumentParser):
        def error(self, message):
            sys.stderr.write(f'error: {message}\n\n')
            self.print_help()
            sys.exit(2)
    parser = MyParser(
        description='Search for zip files in a given S3 bucket on echo.stfc.ac.uk.',
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--bucket-name','-b', type=str, help='Name of the S3 bucket.', required=True)
    parser.add_argument('--list-contents','-l', action='store_true', help='List the contents of the zip files.')
    parser.add_argument('--verify-contents','-v', action='store_true', help='Verify the contents of the zip files from metadata.')
    parser.add_argument('--debug','-d', action='store_true', help='Print debug messages and shorten search.')
    parser.add_argument('--extract','-e', action='store_true', help='Extract and upload zip files for which the contents are not found in the bucket.')

    args = parser.parse_args()
    bucket_name = args.bucket_name
    if args.list_contents:
        list_contents = True
    else:
        list_contents = False
    if args.verify_contents:
        verify_contents = True
    else:
        verify_contents = False
    if args.debug:
        debug = True
    else:
        debug = False
    
    if args.extract:
        extract = True
    else:
        extract = False

    if list_contents or verify_contents or extract:
        get_contents_metadata = True

    # Setup bucket object
    s3_host = 'echo.stfc.ac.uk'
    try:
        keys = bm.get_keys()
    except KeyError as e:
        print(e)
        sys.exit()
    access_key = keys['access_key']
    secret_key = keys['secret_key']
    
    s3 = bm.get_resource(access_key, secret_key, s3_host)
    bucket_list = bm.bucket_list(s3)

    if bucket_name not in bucket_list:
        print(f'Bucket {bucket_name} not found in {s3_host}.')
        sys.exit()

    zipfiles_df, all_keys = get_key_lists(bucket_name, access_key, secret_key, s3_host, get_contents_metadata, debug)

    if list_contents:
        for i in range(len(zipfiles_df)):
            print(f'{zipfiles_df.iloc[i]["zipfile"]}: {zipfiles_df.iloc[i]["contents"]}')
    
    if verify_contents:
        print('Verifying zip file contents...')
        zipfiles_df = prepend_zipfile_path_to_contents(zipfiles_df, debug)
        extract_list = verify_zip_contents(zipfiles_df, all_keys, debug)
        print('Extract List')
        print(extract_list)
        for zipfile in extract_list:
            print(zipfile)
    
    if extract:
        print('Extracting zip files...')
        zipfiles_df = prepend_zipfile_path_to_contents(zipfiles_df, debug)
        extract_list = verify_zip_contents(zipfiles_df, all_keys, debug)
        print(extract_list)
        if len(extract_list) > 0:
            extract_and_upload_zipfiles(extract_list, bucket_name, access_key, secret_key, s3_host, debug)
        

if __name__ == '__main__':
    main()