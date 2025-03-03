import sys
import os
from itertools import repeat
from time import sleep
import warnings
from datetime import datetime, timedelta
import pandas as pd
from ast import literal_eval
import numpy as np
import yaml
import io
import zipfile
import warnings
from psutil import virtual_memory as mem
warnings.filterwarnings('ignore')
from logging import ERROR
import bucket_manager.bucket_manager as bm
import os
import argparse
from dask import dataframe as dd
from distributed import Client, wait
import subprocess
from typing import List
import gc

def logprint(msg,log=None):
    if log is not None:
        with open(log, 'a') as logfile:
            logfile.write(f'{msg}\n')
    else:
        print(msg, flush=True)

def delete_object_swift(obj, s3, log=None):
    deleted = False
    try:
        s3.delete_object(bucket_name, obj)
        logprint(f'Deleted {obj}',log)
        deleted = True
    except Exception as e:
        print(f'Error deleting {obj}: {e}', file=sys.stderr)
    return deleted

def verify_zip_objects(zip_obj, s3, bucket_name, current_objects, log) -> bool:
    """
    Verifies the contents of a zip file based on the provided row and keys series.
    Args:
        row (pd.Series): A pandas Series containing information about the zip file,
                         including 'key', 'is_zipfile', and 'contents'.
        keys_series (pd.Series): A pandas Series containing keys to check against the zip file contents.
    Returns:
        bool: True if the zip file needs to be extracted, False otherwise.
    """
    zip_data = io.BytesIO(s3.get_object(bucket_name, zip_obj)[1])
    with zipfile.ZipFile(zip_data, 'r') as z:
        contents = z.namelist()
    path_stub = '/'.join(zip_obj.split('/')[:-1])
    contents = [f'{path_stub}/{c}' for c in contents]
    verified = False
    if set(contents).issubset(current_objects):
        verified = True
        logprint(f'{zip_obj} verified: {verified} - can be deleted', log)
    else:
        verified = False
        logprint(f'{zip_obj} verified: {verified} - cannot be deleted', log)
    del zip_data, contents
    # gc.collect()
    return verified

if __name__ == '__main__':
    epilog = ''
    class MyParser(argparse.ArgumentParser):
        def error(self, message):
            sys.stderr.write(f'error: {message}\n\n')
            self.print_help()
            sys.exit(2)
    parser = MyParser(
        description='Clean up collated_***.zip objects in S3 bucket, where *** = an integer.',
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('--api', type=str, help='API to use; "S3" or "Swift". Case insensitive. Note: S3 currently not implemented as only Swift is parallelisable through Dask.', default='Swift')
    parser.add_argument('--bucket-name', '-b', type=str, help='Name of the S3 bucket. Required.')
    parser.add_argument('--prefix', '-p', type=str, help='Prefix to be used in S3 object keys. Required.')
    parser.add_argument('--nprocs', '-n', type=int, help='Number of CPU cores to use for parallel upload. Default is 4.', default=4)
    parser.add_argument('--dryrun', '-d', default=False, action='store_true', help='Perform a dry run without uploading files. Default is False.')
    parser.add_argument('--log-to-file', default=False, action='store_true', help='Log output to file. Default is False, i.e., stdout.')
    parser.add_argument('--yes', '-y', default=False, action='store_true', help='Answer yes to all prompts. Default is False.')
    parser.add_argument('--verify', '-v', default=False, action='store_true', help='Verify the contents of the zip file are in the list of uploaded files. Default is False.')
    args = parser.parse_args()

    # Parse arguments
    api = args.api.lower()
    if api not in ['s3', 'swift']:
        parser.error('API must be "S3" or "Swift" (case insensitive).')

    if not args.bucket_name:
        print('Bucket name not provided.', file=sys.stderr)
        parser.print_help()
        sys.exit(1)
    else:
        bucket_name = args.bucket_name
    if not args.prefix:
        print('S3 prefix not provided.', file=sys.stderr)
        parser.print_help()
        sys.exit(1)
    else:
        prefix = args.prefix

    nprocs = args.nprocs
    dryrun = args.dryrun
    yes = args.yes
    log_to_file = args.log_to_file
    verify = args.verify

    if not prefix or not bucket_name:
        parser.print_help()
        sys.exit(1)

    print(f'API: {api}, Bucket name: {bucket_name}, Prefix: {prefix}, nprocs: {nprocs}, dryrun: {dryrun}')

    # Set up logging
    if log_to_file:
        log = f'clean_zips_{bucket_name}_{prefix}_{datetime.now().strftime("%Y%m%d%H%M%S")}.log'
        logprint(f'clean_zips_{bucket_name}_{prefix}_{datetime.now().strftime("%Y%m%d%H%M%S")}.log', log)
        if not os.path.exists(log):
            logprint(f'Created log file {log}',log)
    else:
        log = None
        logprint('Logging to stdout.', log)

    #print hostname
    uname = subprocess.run(['uname', '-n'], capture_output=True)
    logprint(f'Running on {uname.stdout.decode().strip()}', log)

    # Initiate timing
    start = datetime.now()

    # Setup bucket
    try:
        if bm.check_keys(api):
            if api == 's3':
                access_key = os.environ['S3_ACCESS_KEY']
                secret_key = os.environ['S3_ACCESS_KEY']
                s3_host = os.environ['S3_HOST_URL']
            elif api == 'swift':
                access_key = os.environ['ST_USER']
                secret_key = os.environ['ST_KEY']
                s3_host = os.environ['ST_AUTH']
    except AssertionError as e:
        print(f'AssertionError {e}', file=sys.stderr)
        sys.exit()
    except KeyError as e:
        print(f'KeyError {e}', file=sys.stderr)
        sys.exit()
    except ValueError as e:
        print(f'ValueError {e}', file=sys.stderr)
        sys.exit()

    logprint(f'Using {api.capitalize()} API with host {s3_host}')

    if api == 's3':
        print('Currently only Swift is supported for parallelism with Dask. Exiting.', file=sys.stderr)
        sys.exit()

    elif api == 'swift':
        s3 = bm.get_conn_swift()
        bucket_list = bm.bucket_list_swift(s3)

    if bucket_name not in bucket_list:
        print(f'Bucket {bucket_name} not found in {api} bucket list. Exiting.', file=sys.stderr)
        sys.exit()

    if api == 's3':
        bucket = s3.Bucket(bucket_name)
    elif api == 'swift':
        bucket = None

    success = False

    ############################
    #        Dask Setup        #
    ############################
    total_memory = mem().total
    n_workers = nprocs
    mem_per_worker = mem().total//n_workers # e.g., 187 GiB / 48 * 2 = 7.8 GiB
    logprint(f'nprocs: {nprocs}, Threads per worker: 1, Number of workers: {n_workers}, Total memory: {total_memory/1024**3:.2f} GiB, Memory per worker: {mem_per_worker/1024**3:.2f} GiB')

    # Process the files
    with Client(n_workers=n_workers,threads_per_worker=1,memory_limit=mem_per_worker) as client:
        logprint(f'Dask Client: {client}', log=log)
        logprint(f'Dashboard: {client.dashboard_link}', log=log)
        logprint(f'Starting processing at {datetime.now()}, elapsed time = {datetime.now() - start}',log=log)
        logprint(f'Using {nprocs} processes.',log=log)
        logprint(f'Getting current object list for {bucket_name}. This may take some time.\nStarting at {datetime.now()}, elapsed time = {datetime.now() - start}', log=log)

        if api == 's3':
            current_objects = bm.object_list(bucket, prefix=prefix, count=False)
        elif api == 'swift':
            current_objects = bm.object_list_swift(s3, bucket_name, prefix=prefix, count=False)
        logprint(f'Done.\nFinished at {datetime.now()}, elapsed time = {datetime.now() - start}', log=log)

        current_objects = pd.DataFrame.from_dict({'CURRENT_OBJECTS':current_objects})
        logprint(f'Found {len(current_objects)} objects (with matching prefix) in bucket {bucket_name}.', log=log)
        if not current_objects.empty:
            current_zips = current_objects[(current_objects['CURRENT_OBJECTS'].str.contains('collated_\d+\.zip')) & ~(current_objects['CURRENT_OBJECTS'].str.contains('.zip.metadata'))].copy()
            logprint(f'Found {len(current_zips)} zip files (with matching prefix) in bucket {bucket_name}.', log=log)
            # exit()
            if len(current_zips) > 0:
                if verify:
                    current_zips = dd.from_pandas(current_zips, chunksize=100000)
                    current_zips['verified'] = current_zips['CURRENT_OBJECTS'].apply(lambda x: verify_zip_objects(x, s3, bucket_name, current_objects['CURRENT_OBJECTS'], log), meta=('bool'))
                if dryrun:
                    logprint(f'Current objects (with matching prefix): {len(current_objects)}', log=log)
                    if verify:
                        current_zips = current_zips.compute()
                        logprint(f'{len(current_zips[current_zips["verified"] == True])} zip objects were verified as deletable.', log=log)
                    else:
                        logprint(f'Current zip objects (with matching prefix): {len(current_zips)} would be deleted.', log=log)
                    sys.exit()
                else:
                    print(f'Current objects (with matching prefix): {len(current_objects)}')
                    if not verify:
                        print(f'Current zip objects (with matching prefix): {len(current_zips)} will be deleted.')
                    else:
                        print(f'Current zip objects (with matching prefix): {len(current_zips)} will be deleted if all contents exist as objects.')
                    print('WARNING! Files are about to be deleted!')
                    print('Continue [y/n]?')
                    if not yes:
                        if input().lower() != 'y':
                            sys.exit()
                    else:
                        print('auto y')

                    if verify:
                        current_zips['DELETED'] = current_zips[current_zips['verified'] == True]['CURRENT_OBJECTS'].apply(lambda x: delete_object_swift(x, s3, log), meta=('bool'))
                        current_zips['DELETED'] = current_zips[current_zips['verified'] == False]['CURRENT_OBJECTS'] = False
                    else:
                        current_zips['DELETED'] = current_zips['CURRENT_OBJECTS'].apply(lambda x: delete_object_swift(x, s3, log), meta=('bool'))

                    current_zips = current_zips.compute()
                    if current_zips['DELETED'].all():
                        logprint(f'All zip files deleted.', log=log)
                        sys.exit(0)
                    else:
                        logprint(f'Not all zip files were deleted.', log=log)
                        if verify:
                            logprint(f"{len(current_zips[current_zips['verified'] == False])} zip files were not verified and not deleted.", log=log)
                            if len(current_zips[current_zips['verified'] == True]) != len(current_zips[current_zips['DELETED'] == True]):
                                logprint(f"Some errors may have occurred, as some zips verified for deletion were not deleted.", log=log)
                        logprint(f"{len(current_zips[current_zips['DELETED'] == True])} of {len(current_zips)} were deleted.", log=log)
                        sys.exit(0)
            else:
                print(f'No zip files in bucket {bucket_name}. Exiting.')
                sys.exit(0)
        else:
            print(f'No files in bucket {bucket_name}. Exiting.')
            sys.exit(0)
