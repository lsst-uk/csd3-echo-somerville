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

def get_zipfile_list(bucket_name, access_key, secret_key, s3_host, get_contents_metadata):
    zipfile_list = []
    contents_list = []
    s3 = bm.get_resource(access_key, secret_key, s3_host)
    s3_client = bm.get_client(access_key, secret_key, s3_host)
    bucket = s3.Bucket(bucket_name)

    pattern = re.compile(r'.*collated_\d+\.zip$')

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name)

    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if pattern.match(key):
                    zipfile_list.append(key)
                    if get_contents_metadata:
                        contents = bucket.Object(key).get()['Metadata']['zip-contents'].split(',')
                        print(f'{key}: {contents}')
                    else:
                        print(f'{key}')
                        
    
    return zipfile_list, contents

def main():
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
    parser.add_argument('--check-contents','-c', action='store_true', help='Check the contents of the zip files from metadata exist in the bucket.')

    args = parser.parse_args()
    bucket_name = args.bucket_name
    if args.check_contents:
        check_contents = True
    else:
        check_contents = False

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

    zipfile_list, zipfile_contents = get_zipfile_list(bucket_name, access_key, secret_key, s3_host, check_contents)

    if check_contents:
        for i, contents in enumerate(zipfile_contents):
            print(f'Zip file: {zipfile_list[i]}, {contents}')
    else:
        for zipfile in zipfile_list:
            print(zipfile)

if __name__ == '__main__':
    main()