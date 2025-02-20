#!/usr/bin/env python
# coding: utf-8
#D.McKay Jun 2024

from datetime import datetime
import sys
import os

from multiprocessing import cpu_count
from distributed import Client
from distributed import print as dprint
from dask import dataframe as dd
import gc

import pyarrow as pa
import pandas as pd
from numpy.random import randint

import io
import zipfile
import warnings
warnings.filterwarnings('ignore')

import bucket_manager.bucket_manager as bm
import swiftclient
import os
import argparse
import re
import shutil

def get_random_dir() -> str:
    """
    Generates a random file path for a CSV file.
    The function creates a random directory name using a random integer
    between 0 and 1,000,000 (inclusive) and returns a string representing
    the path to a CSV file within that directory.
    Returns:
        str: A string representing the random file path for a CSV file.
    """
    return f'/tmp/{randint(0,1e6):06d}'

def get_random_parquet_path() -> str:
    """
    Generates a random file path for a Parquet file.
    The function creates a random directory name using a random integer
    between 0 and 1,000,000 (inclusive) and returns a string representing
    the path to a Parquet file within that directory.
    Returns:
        str: A string representing the random file path for a Parquet file.
    """
    return f'/tmp/{randint(0,1e6):06d}/data.parquet'

def rm_parquet(path: str) -> None:
    """
    Remove the parent directory of the given path if it is a directory and not a symbolic link.
    If the parent is not a directory but exists, remove it as a file.
    Designed to work with temporary directories created by the get_random_parquet_path function.
    Args:
        path (str): The path to a file or directory whose parent directory is to be removed.
    Returns:
        None
    """

    parent = os.path.dirname(path)
    if os.path.isdir(parent) and not os.path.islink(parent):
        shutil.rmtree(parent)
    elif os.path.exists(parent):
        os.remove(parent)

def find_metadata_swift(key: str, conn: swiftclient.Connection, bucket_name: str) -> str:
    """
    Retrieve metadata for a given key from a Swift container.
    This function attempts to retrieve metadata for a specified key from a Swift container.
    It handles both '.zip' files and other types of files differently. If the key ends with
    '.zip', it tries to fetch the metadata either by getting the object or by checking the
    object's headers. The metadata is expected to be a string separated by '|'.
    Args:
        key (str): The key for which metadata is to be retrieved.
        conn: The connection object to the Swift service.
        container_name (str): The name of the container in the Swift service.
    Returns:
        str: '|'-separated metadata strings if found, otherwise None.
    """

    if type(key) == str:
        existing_zip_contents = None
        if key.endswith('.zip'):
            try:
                existing_zip_contents = str(conn.get_object(bucket_name,''.join([key,'.metadata']))[1].decode('UTF-8')) #.split('|') # use | as separator
                dprint(f'Using zip-contents-object, {"".join([key,".metadata"])} for object {key}.')
            except Exception as e:
                try:
                    existing_zip_contents = conn.head_object(bucket_name,key)['x-object-meta-zip-contents'] #.split('|') # use | as separator
                    dprint(f'Using zip-contents metadata for {key}.')
                except KeyError:
                    return None
                except Exception as e:
                    return None
            if existing_zip_contents is not None:
                if not '|' in existing_zip_contents:
                    existing_zip_contents = '|'.join(existing_zip_contents.split(',')) # some older metadata may have used commas as separators
                return existing_zip_contents
            else:
                return None
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
                dprint(f'Existing objects: {o}', end='\r', flush=True)
    print()
    return keys

def match_key(key):
    pattern = re.compile(r'.*collated_\d+\.zip$')
    if pattern.match(key):
        # dprint(key)
        return True
    else:
        return False

def verify_zip_contents(row: pd.Series, keys_series: pd.Series) -> bool:
    """
    Verifies the contents of a zip file based on the provided row and keys series.
    Args:
        row (pd.Series): A pandas Series containing information about the zip file,
                         including 'key', 'is_zipfile', and 'contents'.
        keys_series (pd.Series): A pandas Series containing keys to check against the zip file contents.
    Returns:
        bool: True if the zip file needs to be extracted, False otherwise.
    """
    contents = row['contents']
    if type(contents) == str:
        contents = contents.split('|')
    extract = False
    if row['is_zipfile']:
        dprint(f'Checking for {row["key"]} contents in all_keys list...')
        if len(contents) > 0:
            if sum(keys_series.isin([contents])) != len(contents):
                extract = True
                dprint(f'{row["key"]} to be extracted.')
            else:
                dprint(f'{row["key"]} contents previously extracted.')
        else:
            extract = True
            dprint(f'{row["key"]} to be extracted (contents unknown).')

    return extract

def prepend_zipfile_path_to_contents(row: pd.Series) -> str:
    """
    Prepends the path of the zip file to the contents of the given row.
    This function takes a pandas Series object representing a row, extracts the path from the 'key' column,
    and prepends it to the 'contents' column. The modified 'contents' value is then returned.
    Args:
        row (pd.Series): A pandas Series object containing at least 'key' and 'contents' columns.
    Returns:
        str: The modified 'contents' value with the prepended path.
    """

    path_stub = '/'.join(row['key'].split('/')[:-1])
    contents = row['contents'].split('|')
    prepended = [f'{path_stub}/{c}' for c in contents]
    return '|'.join(prepended)

def extract_and_upload(key: str, conn: swiftclient.Connection, bucket_name: str) -> bool:
    """
    Extracts the contents of a zip file from an object storage bucket and uploads the extracted files back to the bucket.
    Args:
        key (str): A zip file object 'path' on S3 storage.
        conn (swiftclient.Connection): A connection object to interact with the object storage service.
        bucket_name (str): The name of the bucket where the zip file is stored and where the extracted files will be uploaded.
    Returns:
        bool: True if the extraction and upload process was successful, False otherwise.
    """
    done = False
    start = datetime.now()
    size = 0
    # dprint(f'Extracting {row["key"]}...', flush=True)
    path_stub = '/'.join(key.split('/')[:-1])
    zipfile_data = io.BytesIO(conn.get_object(bucket_name,key)[1])
    with zipfile.ZipFile(zipfile_data) as zf:
        for content_file in zf.namelist():
            # dprint(content_file, flush=True)
            content_file_data = zf.open(content_file)
            size += len(content_file_data.read())
            key = path_stub + '/' + content_file
            conn.put_object(bucket_name,key,content_file_data)
            done = True
            del content_file_data
            # dprint(f'Uploaded {content_file} to {key}', flush=True)
    del zipfile_data
    gc.collect()
    end = datetime.now()
    duration = (end - start).microseconds / 1e6 + (end - start).seconds
    try:
        dprint(f'Extracted and uploaded contents of {key} in {duration:.2f} s ({(size/1024**2/duration):.2f} MiB/s).', flush=True)
    except ZeroDivisionError:
        dprint(f'Extracted and uploaded contents of {key} in {duration:.2f} s', flush=True)

    return done


def main():
    """
    Main function to process collated zip files in an S3 bucket on echo.stfc.ac.uk.
    This script performs the following tasks:
    1. Parses command-line arguments to determine the S3 bucket name, whether to list zip files,
        whether to extract and upload zip files, and the maximum number of processes to use.
    2. Validates the provided arguments and sets up the necessary configurations.
    3. Connects to the specified S3 bucket and retrieves the list of objects.
    4. Uses Dask to parallelize the processing of the objects in the bucket.
    5. Identifies zip files and optionally lists them.
    6. Extracts and uploads the contents of the zip files if specified.
    Command-line arguments:
         --bucket-name, -b: Name of the S3 bucket (required).
         --list-zips, -l: List the zip files in the bucket.
         --extract, -e: Extract and upload zip files whose contents are not found in the bucket.
         --nprocs, -n: Maximum number of processes to use for extraction and upload (default: 6).
    Raises:
         SystemExit: If invalid arguments are provided or if the bucket is not found.
    """
    all_start = datetime.now()
    print(f'Start time: {all_start}.')
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
    threads_per_worker = 8
    n_workers = nprocs//threads_per_worker
    mem_per_worker = 64*1024**3//n_workers # limit to 64 GiB total memory

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
        dprint(f'Dask Client: {client}', flush=True)
        dprint(f'Dashboard: {client.dashboard_link}', flush=True)
        dprint(f'Using {n_workers} workers, each with {threads_per_worker} threads, on {nprocs} CPUs.')

        #Dask Dataframe of all keys
        keys_df = dd.from_pandas(keys, chunksize=1000000)
        if extract:
            keys_only_df = keys_df['key'].copy()
            keys_only_df = client.persist(keys_only_df)
        del keys
        # dprint(keys_df)
        #Discover if key is a zipfile
        keys_df['is_zipfile'] = keys_df['key'].apply(match_key, meta=('is_zipfile', 'bool'))

        #check, compute and write to parquet
        # keys_df = client.persist(keys_df) # persist to memory to make the following faster

        if list_zips:
            keys_df = keys_df.compute() # effectively to_pandas
            check = keys_df['is_zipfile'].any()
            if not check:
                dprint('No zipfiles found. Exiting.')
                sys.exit()
            dprint('Zip files found:')
            dprint(keys_df[keys_df['is_zipfile'] == True]['key'])
            print(f'Done. Runtime: {datetime.now() - all_start}.')
            sys.exit()

        if extract:
            # keys_df = client.persist(keys_df)
            check = keys_df['is_zipfile'].any().compute()
            if not check:
                dprint('No zipfiles found. Exiting.')
                sys.exit()
            #Get metadata for zipfiles
            keys_df['contents'] = keys_df[keys_df['is_zipfile'] == True]['key'].apply(find_metadata_swift, conn=conn, bucket_name=bucket_name, meta=('contents', 'str'))
            keys_df[keys_df['is_zipfile'] == False]['contents'] = ''

            #Prepend zipfile path to contents
            # dprint(keys_df)
            keys_df[keys_df['is_zipfile'] == True]['contents'] = keys_df[keys_df['is_zipfile'] == True].apply(prepend_zipfile_path_to_contents, meta=('contents', 'str'), axis=1)
            keys_series = keys_df['key'].compute()
            keys_df['extract'] = keys_df.apply(verify_zip_contents, meta=('extract', 'bool'), keys_series=keys_series, axis=1)

            # all wrangling and decision making done - write to parquet for lazy unzipping
            # only require key and extract boolean
            pq = get_random_parquet_path()
            dprint(f'tmp folder is {pq}')
            keys_df = keys_df[['key','extract']]
            keys_df.to_parquet(pq, schema=pa.schema([
                ('key', pa.string()),
                ('extract', pa.bool_()),
            ]))
            del keys_df, keys_series

            dprint('Extracting zip files...')
            keys_df = dd.read_parquet(pq,dtype={'key':'str', 'extract': 'bool'}, chunksize=100000) # small chunks to avoid memory issues

            dprint('Zip files extracted and uploaded:')
            keys_df['extracted_and_uploaded'] = keys_df[keys_df['extract'] == True]['key'].apply(extract_and_upload, conn=conn, bucket_name=bucket_name, meta=('extracted_and_uploaded', 'bool'), axis=1)
            extracted_and_uploaded = keys_df[keys_df['extract'] == True]['extracted_and_uploaded'].compute(resources={'MEMORY': 10e9})
            del(keys_df)
            rm_parquet(pq)
            if extracted_and_uploaded.all():
                dprint('All zip files extracted and uploaded.')

    print(f'Done. Runtime: {datetime.now() - all_start}.')

if __name__ == '__main__':
    main()