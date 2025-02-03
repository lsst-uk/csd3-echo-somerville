#!/usr/bin/env python
# coding: utf-8
#D.McKay Jun 2024


import sys
import os
from distributed import Client, wait

from multiprocessing import cpu_count
from distributed import Client
from dask import dataframe as dd
import dask
import pandas as pd

from time import sleep
from psutil import virtual_memory as mem
import io
import zipfile
import warnings
warnings.filterwarnings('ignore')

import bucket_manager.bucket_manager as bm
import swiftclient
import os
import argparse
from typing import List
import re

def find_metadata_swift(key: str, conn, bucket_name: str) -> List[str]:
    """
    Retrieve metadata for a given key from a Swift container.
    This function attempts to retrieve metadata for a specified key from a Swift container.
    It handles both '.zip' files and other types of files differently. If the key ends with
    '.zip', it tries to fetch the metadata either by getting the object or by checking the
    object's headers. The metadata is expected to be a string separated by '|' or ','.
    Args:
        key (str): The key for which metadata is to be retrieved.
        conn: The connection object to the Swift service.
        container_name (str): The name of the container in the Swift service.
    Returns:
        List[str]: A list of metadata strings if found, otherwise None.
    """

    if type(key) == str:
        existing_zip_contents = None
        if key.endswith('.zip'):
            try:
                existing_zip_contents = str(conn.get_object(bucket_name,''.join([key,'.metadata']))[1].decode('UTF-8')).split('|') # use | as separator
                print(f'Using zip-contents-object, {"".join([key,".metadata"])} for object {key}.')
            except Exception as e:
                try:
                    existing_zip_contents = conn.head_object(bucket_name,key)['x-object-meta-zip-contents'].split('|') # use | as separator
                    print(f'Using zip-contents metadata for {key}.')
                except KeyError:
                    return None
                except Exception as e:
                    return None
            if existing_zip_contents:
                if len(existing_zip_contents) == 1:
                        existing_zip_contents = existing_zip_contents[0].split(',') # revert to comma if no | found
                return existing_zip_contents
        else:
            return None
    else:
        return None

def object_list_swift(conn: swiftclient.Connection, container_name: str, prefix : str = '', full_listing: bool = True, count: bool = False) -> list[str]:
    """
    Returns a list of keys of all objects in the specified bucket.

    Parameters:
    - conn: swiftlient.Connection object.
    - container_name: The name of the Swift container.

    Returns:
    A list of object keys.
    """
    keys = []
    if count:
        o = 0
    for obj in conn.get_container(container_name,prefix=prefix,full_listing=full_listing)[1]:
        keys.append(obj['name'])
        if count:
            o += 1
            if o % 10000 == 0:
                print(f'Existing objects: {o}', end='\r', flush=True)
    return keys

def match_key(key):
    pattern = re.compile(r'.*collated_\d+\.zip$')
    if pattern.match(key):
        print(key)
        return True
    else:
        return False

def verify_zip_contents(row, keys_df):
    """
    Verify the contents of a zipfile against a list of keys.

    Parameters:
    - row (pandas.Series): The row containing the zipfile information.
    - keys_df (pandas.DataFrame): The dataframe containing the zipfile information.

    Returns:
    - extract (bool): True if the zipfile contents are not found in the keys_df, otherwise False.
    """
    extract = False
    # print(zipfiles_df)
    # print(all_keys)
    print('Checking for zipfile contents in all_keys list...')

    if row['is_zipfile']:
        if row['contents']:
            if sum(keys_df['key'].isin([row['contents']])) != len(row['contents']):
                extract = True
                print(f'{row["key"]} to be extracted.')
            else:
                print(f'{row["key"]} contents previously extracted.')
        else:
            extract = True
            print(f'{row["key"]} to be extracted (contents unknown).')

    return extract

def prepend_zipfile_path_to_contents(row):
    """
    Prepend the path to the zipfile to the contents column in the given DataFrame.
    Contents by default are relative to the zipfile location.
    Parameters:
    - zipfile_df (DataFrame): The DataFrame containing the zipfile information.
    Returns:
    - None
    """
    path_stub = '/'.join(row['key'].split('/')[:-1])
    row['contents'] = f'{path_stub}/{row["contents"]}'
    return row['contents']

def extract_and_upload(row, conn, bucket_name):
    if row['extract']:
        print(f'Extracting {row["key"]}...', flush=True)
        path_stub = '/'.join(row["key"].split('/')[:-1])
        zipfile_data = io.BytesIO(conn.get_object(bucket_name,row['key'])[1])
        with zipfile.ZipFile(zipfile_data) as zf:
            for content_file in zf.namelist():
                print(content_file, flush=True)
                content_file_data = zf.open(content_file)
                key = path_stub + '/' + content_file
                conn.put_object(bucket_name,key,content_file_data)
                print(f'Uploaded {content_file} to {key}', flush=True)
        return True
    else:
        return False

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
    parser.add_argument('--list-zips','-l', action='store_true', help='List the zip files.')
    parser.add_argument('--extract','-e', action='store_true', help='Extract and upload zip files for which the contents are not found in the bucket.')
    parser.add_argument('--nprocs','-n', type=int, help='Maximum number of processes to use for extraction and upload.', default=6)

    args = parser.parse_args()
    bucket_name = args.bucket_name
    if args.list_zips:
        list_zips = True
    else:
        list_zips = False

    if args.extract:
        extract = True
    else:
        extract = False

    if list_zips and extract:
        print('Cannot list contents and extract at the same time. Exiting.')
        sys.exit()

    if args.nprocs:
        nprocs = args.nprocs
        if nprocs < 1:
            nprocs = 1
        elif nprocs > (cpu_count() - 1): # allow one core for __main__ and system processes
            print(f'Maximum number of processes set to {nprocs} but only {cpu_count() - 1} available. Setting to {cpu_count() - 1}.')
            nprocs = cpu_count() - 1
    else:
        nprocs = 6
    threads_per_worker = 1
    n_workers = nprocs//threads_per_worker
    mem_per_worker = 64*1024**3/n_workers # limit to 64 GiB total memory

    # Setup bucket object
    try:
        assert bm.check_keys(api='swift')
    except AssertionError as e:
        print(e)
        sys.exit()

    conn = bm.get_conn_swift()
    bucket_list = bm.bucket_list_swift(conn)

    if bucket_name not in bucket_list:
        print(f'Bucket {bucket_name} not found in {os.environ["ST_AUTH"]}.')
        sys.exit()

    print(f'Using bucket {bucket_name}.')

    print('Getting key list...')
    keys = pd.DataFrame.from_dict({'key':object_list_swift(conn, bucket_name, count=True)})
    print(keys.head())

    with Client(n_workers=n_workers,threads_per_worker=threads_per_worker,memory_limit=mem_per_worker) as client:
        # dask.set_options(get=dask.local.get_sync)
        print(f'Dask Client: {client}', flush=True)
        # print(f'Dashboard: {client.dashboard_link}', flush=True)
        print(f'Using {n_workers} workers, each with {threads_per_worker} threads, on {nprocs} CPUs.')

        #Dask Dataframe of all keys
        keys_df = dd.from_pandas(keys, npartitions=len(keys)//n_workers)
        del keys
        print(keys_df)
        #Discover if key is a zipfile
        keys_df['is_zipfile'] = keys_df['key'].apply(match_key, meta=('is_zipfile', 'bool'))
        if not keys_df['is_zipfile'].any().compute():
            print('No zipfiles found. Exiting.')
            sys.exit()
        #Get metadata for zipfiles
        print(keys_df) # make sure not to imply .compute by printing!
        keys_df['contents'] = keys_df[keys_df['is_zipfile'] == True]['key'].apply(find_metadata_swift, conn=conn, bucket_name=bucket_name, meta=('contents', 'str'))
        #Prepend zipfile path to contents
        keys_df[keys_df['is_zipfile'] == True]['contents'] = keys_df[keys_df['is_zipfile'] == True].apply(prepend_zipfile_path_to_contents, meta=('contents', 'str'), axis=1)
        #Set contents to None for non-zipfiles
        keys_df[keys_df['is_zipfile'] == False]['contents'] = None

        if list_zips:
            print(keys_df[keys_df['is_zipfile'] == True]['key'].compute(scheduler='processes'))

        if extract:
            print('Extracting zip files...')
            keys_df['extract'] = keys_df.apply(verify_zip_contents, meta=('extract', 'bool'), keys_df=keys_df, axis=1)
            keys_df['extracted and uploaded'] = keys_df.apply(extract_and_upload, conn=conn, bucket_name=bucket_name, meta=('extracted and uploaded', 'bool'), axis=1)
            print('Zip files extracted and uploaded:')
            print(keys_df[keys_df['extracted and uploaded'] == True]['key'].compute(scheduler='processes'))

    print('Done.')

if __name__ == '__main__':
    main()