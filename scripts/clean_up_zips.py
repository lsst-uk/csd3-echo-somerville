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
    parser.add_argument('--api', type=str, help='API to use; "S3" or "Swift". Case insensitive.')
    parser.add_argument('--bucket-name', type=str, help='Name of the S3 bucket.')
    parser.add_argument('--S3-prefix', type=str, help='Prefix to be used in S3 object keys.')
    parser.add_argument('--nprocs', type=int, help='Number of CPU cores to use for parallel upload.')
    parser.add_argument('--dryrun', default=False, action='store_true', help='Perform a dry run without uploading files.')
    args = parser.parse_args()

    if not args.config_file and not (args.bucket_namse and args.local_path and args.S3_prefix):
        parser.error('If a config file is not provided, the bucket name, local path, and S3 prefix must be provided.')
    if args.config_file and (args.api or
                             args.bucket_name or
                             args.local_path or
                             args.S3_prefix or
                             args.S3_folder or
                             args.exclude or
                             args.nprocs or
                             args.threads_per_worker or
                             args.no_collate or
                             args.dryrun or
                             args.no_compression or
                             args.save_config or
                             args.no_file_count_stop):
        print(f'WARNING: Options provide on command line override options in {args.config_file}.')
    if args.config_file:
        config_file = args.config_file
        if not os.path.exists(config_file) and not args.save_config:
            sys.exit(f'Config file {config_file} does not exist.')
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
                print(config)
                if 'bucket_name' in config.keys() and not args.bucket_name:
                    args.bucket_name = config['bucket_name']
                if 'local_path' in config.keys() and not args.local_path:
                    args.local_path = config['local_path']
                if 'S3_prefix' in config.keys() and not args.S3_prefix:
                    args.S3_prefix = config['S3_prefix']
                if 'S3_folder' in config.keys() and not args.S3_folder:
                    args.S3_folder = config['S3_folder']
                if 'exclude' in config.keys() and not args.exclude:
                    args.exclude = config['exclude']
                if 'nprocs' in config.keys() and not args.nprocs:
                    args.nprocs = config['nprocs']
                if 'nprocs' not in config.keys() and not args.nprocs: # required to allow default value of 4 as this overrides "default" in add_argument
                    args.nprocs = 4
                if 'threads_per_worker' in config.keys() and not args.threads_per_worker:
                    args.threads_per_worker = config['threads_per_worker']
                if 'threads_per_worker' not in config.keys() and not args.threads_per_worker: # required to allow default value of 4 as this overrides "default" in add_argument
                    args.threads_per_worker = 2
                if 'no_checksum' in config.keys() and not args.no_checksum:
                    args.no_checksum = config['no_checksum']
                if 'no_collate' in config.keys() and not args.no_collate:
                    args.no_collate = config['no_collate']
                if 'dryrun' in config.keys() and not args.dryrun:
                    args.dryrun = config['dryrun']
                if 'no_compression' in config.keys() and not args.no_compression:
                    args.no_compression = config['no_compression']
                if 'no_file_count_stop' in config.keys() and not args.no_file_count_stop:
                    args.no_file_count_stop = config['no_file_count_stop']
                if 'api' in config.keys() and not args.api:
                    args.api = config['api']
                if 'api' not in config.keys() and not args.api:
                    args.api = 's3'

    if args.save_config and not args.config_file:
        parser.error('A config file must be provided to save the configuration.')

    no_checksum = args.no_checksum
    if no_checksum:
        parser.error('Please note: the optin to disable file checksum has been deprecated.')

    save_config = args.save_config
    api = args.api.lower()
    if api not in ['s3', 'swift']:
        parser.error('API must be "S3" or "Swift" (case insensitive).')
    bucket_name = args.bucket_name
    local_dir = args.local_path
    if not os.path.exists(local_dir):
        sys.exit(f'Local path {local_dir} does not exist.')
    prefix = args.S3_prefix
    sub_dirs = args.S3_folder
    print(f'sub_dirs {sub_dirs}')
    nprocs = args.nprocs
    threads_per_worker = args.threads_per_worker
    print(f'threads per worker: {threads_per_worker}')
    global_collate = not args.no_collate # internally, flag turns *on* collate, but for user no-collate turns it off - makes flag more intuitive
    dryrun = args.dryrun
    use_compression = not args.no_compression # internally, flag turns *on* compression, but for user no-compression turns it off - makes flag more intuitive

    file_count_stop = not args.no_file_count_stop  # internally, flag turns *on* file-count-stop, but for user no-file-count-stop turns it off - makes flag more intuitive

    if args.exclude:
        exclude = pd.Series(args.exclude)
    else:
        exclude = pd.Series([])

    print(f'Config: {args}')

    if save_config:
        with open(config_file, 'w') as f:
            yaml.dump({
                'bucket_name': bucket_name,
                'api': api,
                'local_path': local_dir,
                'S3_prefix': prefix,
                'S3_folder': sub_dirs,
                'nprocs': nprocs,
                'threads_per_process': threads_per_worker,
                'no_collate': not global_collate,
                'dryrun': dryrun,
                'no_compression': not use_compression,
                'no_file_count_stop': not file_count_stop,
                'exclude': exclude.to_list(),
                }, f)
        sys.exit(0)

    print(f'Symlinks will be replaced with the target file. A new file <simlink_file>.symlink will contain the symlink target path.')

    if not local_dir or not prefix or not bucket_name:
        parser.print_help()
        sys.exit(1)

    #print hostname
    uname = subprocess.run(['uname', '-n'], capture_output=True)
    print(f'Running on {uname.stdout.decode().strip()}')

    # Initiate timing
    start = datetime.now()

    ##allow top-level folder to be provided with S3-folder == ''
    if sub_dirs == '':
        log_suffix = 'lsst-backup.csv' # DO NOT CHANGE
        log = f"{prefix}-{log_suffix}"
        # check for previous suffix (remove after testing)
        previous_suffix = 'files.csv'
        previous_log = f"{prefix}-{previous_suffix}"
        destination_dir = f"{prefix}"
    else:
        log_suffix = 'lsst-backup.csv' # DO NOT CHANGE
        log = f"{prefix}-{'-'.join(sub_dirs.split('/'))}-{log_suffix}"
        # check for previous suffix (remove after testing)
        previous_suffix = 'files.csv'
        previous_log = f"{prefix}-{'-'.join(sub_dirs.split('/'))}-{previous_suffix}"
        destination_dir = f"{prefix}/{sub_dirs}"

    if global_collate:
        collate_list_suffix = 'collate-list.csv'
        collate_list_file = log.replace(log_suffix,collate_list_suffix) # now automatically generated
        save_collate_list = True # no longer optional
        if save_collate_list and not os.path.exists(collate_list_file):
            print(f'Collate list will be generated and saved to {collate_list_file}.')
        elif save_collate_list and os.path.exists(collate_list_file):
            print(f'Collate list will be read from and re-saved to {collate_list_file}.')

    # Add titles to log file
    if not os.path.exists(log):
        if os.path.exists(previous_log):
            # rename previous log
            os.rename(previous_log, log)
            print(f'Renamed {previous_log} to {log}')
        else:
            # create new log
            print(f'Created backup log file {log}')
            with open(log, 'a') as logfile: # don't open as 'w' in case this is a continuation
                logfile.write('LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,ZIP_CONTENTS\n')

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
        s3 = bm.get_resource()
        bucket_list = bm.bucket_list(s3)
    elif api == 'swift':
        s3 = bm.get_conn_swift()
        bucket_list = bm.bucket_list_swift(s3)

    if bucket_name not in bucket_list:
        if not dryrun:
            if api == 's3':
                s3.create_bucket(Bucket=bucket_name)
            elif api == 'swift':
                s3.put_container(bucket_name)
            print(f'Added bucket: {bucket_name}')
    else:
        if not dryrun:
            print(f'Bucket exists: {bucket_name}')
            print('Existing files will be skipped.')
        else:
            print(f'Bucket exists: {bucket_name}')
            print('dryrun == True, so continuing.')

    if api == 's3':
        bucket = s3.Bucket(bucket_name)
    elif api == 'swift':
        bucket = None

    success = False


    ############################
    #        Dask Setup        #
    ############################
    total_memory = mem().total
    n_workers = nprocs//threads_per_worker
    mem_per_worker = mem().total//n_workers # e.g., 187 GiB / 48 * 2 = 7.8 GiB
    print(f'nprocs: {nprocs}, Threads per worker: {threads_per_worker}, Number of workers: {n_workers}, Total memory: {total_memory/1024**3:.2f} GiB, Memory per worker: {mem_per_worker/1024**3:.2f} GiB')

    # Process the files
    with Client(n_workers=n_workers,threads_per_worker=threads_per_worker,memory_limit=mem_per_worker) as client:
        print(f'Dask Client: {client}', flush=True)
        print(f'Dashboard: {client.dashboard_link}', flush=True)
        print(f'Starting processing at {datetime.now()}, elapsed time = {datetime.now() - start}')
        print(f'Using {nprocs} processes.')
        print(f'Getting current object list for {bucket_name}. This may take some time.\nStarting at {datetime.now()}, elapsed time = {datetime.now() - start}', flush=True)

        if api == 's3':
            current_objects = bm.object_list(bucket, prefix=destination_dir, count=True)
        elif api == 'swift':
            current_objects = bm.object_list_swift(s3, bucket_name, prefix=destination_dir, count=True)
        print()
        print(f'Done.\nFinished at {datetime.now()}, elapsed time = {datetime.now() - start}', flush=True)

        current_objects = pd.DataFrame.from_dict({'CURRENT_OBJECTS':current_objects})

        print(f'Current objects (with matching prefix): {len(current_objects)}', flush=True)