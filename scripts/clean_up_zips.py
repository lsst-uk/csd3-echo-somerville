import sys
import os
from itertools import repeat
from time import sleep
import warnings
from datetime import datetime, timedelta
import hashlib
import base64
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

import swiftclient

import hashlib
import os
import argparse
from dask import dataframe as dd
from dask.distributed import Client, get_client, wait, as_completed, Future, fire_and_forget
from dask.distributed import print as dprint
import subprocess

from typing import List

def logprint(msg,log):
    with open(log, 'a') as logfile:
        logfile.write(f'{msg}\n')

def delete_object_swift(obj):
    success = False
    try:
        s3.delete_object(bucket_name, obj)
        success = True
    except Exception as e:
        print(f'Error deleting {obj}: {e}', file=sys.stderr)
    return success

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
    parser.add_argument('--api', type=str, help='API to use; "S3" or "Swift". Case insensitive. Note: Swift is required for parallelism with Dask.')
    parser.add_argument('--bucket-name', type=str, help='Name of the S3 bucket.')
    parser.add_argument('--S3-prefix', type=str, help='Prefix to be used in S3 object keys.')
    parser.add_argument('--nprocs', type=int, help='Number of CPU cores to use for parallel upload.')
    parser.add_argument('--dryrun', default=False, action='store_true', help='Perform a dry run without uploading files.')
    parser.add_argument('--log-to-file', default=False, action='store_true', help='Log output to file.')
    parser.add_argument('--yes', '-y', default=False, action='store_true', help='Answer yes to all prompts.')
    args = parser.parse_args()

    # Parse arguments
    api = args.api.lower()
    if api not in ['s3', 'swift']:
        parser.error('API must be "S3" or "Swift" (case insensitive).')
    bucket_name = args.bucket_name
    prefix = args.S3_prefix
    nprocs = args.nprocs
    dryrun = args.dryrun
    yes = args.yes
    log_to_file = args.log_to_file

    if not prefix or not bucket_name:
        parser.print_help()
        sys.exit(1)

    print(f'API: {api}, Bucket name: {bucket_name}, Prefix: {prefix}, nprocs: {nprocs}, dryrun: {dryrun}')
    if not dryrun:
        print('WARNING! This is not a dry run. Files will be deleted.')
        if not yes:
            print('Continue [y/n]?')
            if input().lower() != 'y':
                parser.print_help()
                sys.exit()

    # Set up logging
    if log_to_file:
        if not os.path.exists(log):
            print(f'Created log file {log}')
        log = f'clean_zips_{bucket_name}_{prefix}_{datetime.now().strftime("%Y%m%d%H%M%S")}.log'
    else:
        log = sys.stdout
    logprint(f'clean_zips_{bucket_name}_{prefix}_{datetime.now().strftime("%Y%m%d%H%M%S")}.log', log)


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

    print(f'Using {api.capitalize()} API with host {s3_host}')

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
    print(f'nprocs: {nprocs}, Threads per worker: 1, Number of workers: {n_workers}, Total memory: {total_memory/1024**3:.2f} GiB, Memory per worker: {mem_per_worker/1024**3:.2f} GiB')

    # Process the files
    with Client(n_workers=n_workers,threads_per_worker=1,memory_limit=mem_per_worker) as client:
        print(f'Dask Client: {client}', flush=True)
        print(f'Dashboard: {client.dashboard_link}', flush=True)
        print(f'Starting processing at {datetime.now()}, elapsed time = {datetime.now() - start}')
        print(f'Using {nprocs} processes.')
        print(f'Getting current object list for {bucket_name}. This may take some time.\nStarting at {datetime.now()}, elapsed time = {datetime.now() - start}', flush=True)

        if api == 's3':
            current_objects = bm.object_list(bucket, prefix=prefix, count=True)
        elif api == 'swift':
            current_objects = bm.object_list_swift(s3, bucket_name, prefix=prefix, count=True)
        print()
        print(f'Done.\nFinished at {datetime.now()}, elapsed time = {datetime.now() - start}', flush=True)

        current_objects = pd.DataFrame.from_dict({'CURRENT_OBJECTS':current_objects})

        current_zips = dd.from_pandas(current_objects[current_objects['CURRENT_OBJECTS'].str.endswith('collated_\d+\.zip')]['CURRENT_OBJECTS'], npartitions=n_workers)

        if dryrun:
            print(f'Current objects (with matching prefix): {len(current_objects)}', flush=True)
            print(f'Current zip objects (with matching prefix): {len(current_zips)} would be deleted.', flush=True)
            sys.exit()
        else:
            print(f'Current objects (with matching prefix): {len(current_objects)}', flush=True)
            print(f'Current zip objects (with matching prefix): {len(current_zips)} will be deleted.', flush=True)

        current_zips['deleted'] = current_zips.map_partitions(lambda df: df.apply(lambda x: delete_object_swift(x), axis=1), meta=('x', 'bool'))
        current_zips.compute()
        if current_zips['deleted'].all():
            print(f'All zip files deleted.', flush=True, file=log)
            sys.exit(0)
        else:
            print(f'Error deleting zip files.', flush=True, file=log)
            print(f'{len(current_zips['deleted' == False]['CURRENT_OBJECTS'])} / {len(current_zips)} deleted', flush=True, file=log)
            sys.exit(1)