#!/usr/bin/env python
# coding: utf-8
#D.McKay Jun 2024


import sys
import os
from multiprocessing import Pool
from multiprocessing import cpu_count
from itertools import repeat
from functools import partial
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
from tqdm import tqdm

import bucket_manager.bucket_manager as bm

import hashlib
import os
import argparse

import re

def get_key_lists(bucket_name, get_contents_metadata, debug):
    zipfile_list = []
    contents_list = []
    zipfile_sizes = []
    all_keys_list = []
    s3 = bm.get_resource()
    s3_client = bm.get_client()
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
                        try:
                            metadata = bucket.Object(key).get()['Metadata']
                            if 'zip-contents' in metadata:
                                print('Using zip-contents metadata.')
                                contents = metadata['zip-contents'].split('|') # use | as separator
                                contents_list.append(contents)
                            elif 'zip-contents-object' in metadata:
                                print('Using zip-contents-object.')
                                contents = metadata['zip-contents-object']
                                contents_object = bucket.Object(contents).get()['Body'].read().decode('utf-8').split('|') # use | as separator
                                contents_list.append(contents_object)
                            else:
                                raise KeyError(f'Key {key} has no zip-contents metadata.')

                        except KeyError as e:
                            print(f'Key {key} has no zip-contents metadata.')
                            contents_list.append([])
                        zipfile_sizes.append(obj['Size'])
                        # print(f'{key}: {contents}')
                    # else:
                else:
                    all_keys_list.append(key)
            print(f'Keys found: {key_count}, Zip files found: {zipfile_count}', end='\r')
            # for debugging
            if debug:
                if key_count >= 200:
                    break
    print()
    zipfile_df = pd.DataFrame(np.array([zipfile_list,zipfile_sizes,contents_list], dtype=object).T, columns=['zipfile','size','contents'])
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
        if debug:
            extract_list.append(zipfiles_df.iloc[i]["zipfile"])
            break
        if sum(all_keys.isin(zipfiles_df['contents'].iloc[i])) != len(zipfiles_df['contents'].iloc[i]):
            extract_list.append(zipfiles_df.iloc[i]["zipfile"])
            print(f'{zipfiles_df.iloc[i]["zipfile"]} to be extracted.')
        elif len(zipfiles_df['contents'].iloc[i]) == 0:
            extract_list.append(zipfiles_df.iloc[i]["zipfile"])
            print(f'{zipfiles_df.iloc[i]["zipfile"]} to be extracted (contents unknown).')
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

def extract_and_upload_mp(bucket_name, debug, zipfile_key):
    s3 = bm.get_resource()
    bucket = s3.Bucket(bucket_name)
    print(f'Extracting {zipfile_key}...', flush=True)
    path_stub = '/'.join(zipfile_key.split('/')[:-1])
    zipfile_data = io.BytesIO(bucket.Object(zipfile_key).get()['Body'].read())
    total_size = zipfile_data.getbuffer().nbytes
    with zipfile.ZipFile(zipfile_data) as zf:
        with tqdm(total=total_size, desc='Uploading', unit='B', unit_scale=True) as pbar:
            for content_file in zf.namelist():
                print(content_file, flush=True)
                content_file_data = zf.open(content_file)
                key = path_stub + '/' + content_file
                bucket.upload_fileobj(content_file_data, f'{key}')
                print(f'Uploaded {content_file} to {key}', flush=True)
                pbar.update(zf.getinfo(content_file).file_size)

def extract_and_upload_zipfiles(extract_list, bucket_name, pool_size, debug):
    print(f'Extracting zip files and uploading contents using {pool_size} processes...')
    with Pool(pool_size) as p:
        p.map(partial(extract_and_upload_mp, bucket_name, debug), extract_list)#, chunksize=len(extract_list)//pool_size)

def calc_pool_size(zipfiles_df, extract_list, nprocs):
    """
    Calculate the number of processes to use for extraction and upload.
    Ensures number of processors is maximised while not exceeding available memory.
    Lower of nprocs and calculated max procs is returned.

    Parameters:
    - zipfiles_df (pandas.DataFrame): The DataFrame containing the zipfile information.
    - extract_list (list): The list of zipfiles to extract.
    - nprocs (int): The number of processes to use.

    Returns:
    - int: The number of processes to use.
    """
    from psutil import virtual_memory
    largest_zipfile = zipfiles_df[zipfiles_df['zipfile'].isin(extract_list)]['size'].max()
    available_memory = virtual_memory().available
    if largest_zipfile > available_memory:
        print('Largest zipfile exceeds available memory. Reducing number of processes.')
        return 1
    else:
        memory_ratio = int(available_memory / largest_zipfile)
        print(f'Largest zipfile ({largest_zipfile}) fits in available memory ({available_memory}) {memory_ratio} times.')
        return min([nprocs * memory_ratio, nprocs])


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
    parser.add_argument('--nprocs','-n', type=int, help='Maximum number of processes to use for extraction and upload.', default=6)

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

    if args.nprocs:
        nprocs = args.nprocs
        if nprocs < 1:
            nprocs = 1
        elif nprocs > (cpu_count() - 1): # allow one core for __main__ and system processes
            print(f'Maximum number of processes set to {nprocs} but only {cpu_count() - 1} available. Setting to {cpu_count() - 1}.')
            nprocs = cpu_count() - 1
    else:
        nprocs = 6

    if list_contents or verify_contents or extract:
        get_contents_metadata = True

    # Setup bucket object
    try:
        assert bm.check_keys()
    except AssertionError as e:
        print(e)
        sys.exit()

    s3 = bm.get_resource()
    bucket_list = bm.bucket_list(s3)

    if bucket_name not in bucket_list:
        print(f'Bucket {bucket_name} not found in {os.environ['S3_HOST_URL']}.')
        sys.exit()

    zipfiles_df, all_keys = get_key_lists(bucket_name, get_contents_metadata, debug)

    if list_contents:
        for i in range(len(zipfiles_df)):
            print(f'{zipfiles_df.iloc[i]["zipfile"]}: {zipfiles_df.iloc[i]["contents"]}')

    if verify_contents:
        print('Verifying zip file contents...')
        zipfiles_df = prepend_zipfile_path_to_contents(zipfiles_df, debug)
        extract_list = verify_zip_contents(zipfiles_df, all_keys, debug)
        if len(extract_list) > 0:
            print('Some zip files still to extract:')
            print(extract_list)
        else:
            print('All zip files previously extracted.')

    if extract:
        print('Extracting zip files...')
        zipfiles_df = prepend_zipfile_path_to_contents(zipfiles_df, debug)
        extract_list = verify_zip_contents(zipfiles_df, all_keys, debug)

        print(extract_list)
        if len(extract_list) > 0:
            pool_size = calc_pool_size(zipfiles_df, extract_list, nprocs)
            extract_and_upload_zipfiles(extract_list, bucket_name, pool_size, debug)
        else:
            print('All zip files previously extracted.')

    print('Done.')

if __name__ == '__main__':
    main()