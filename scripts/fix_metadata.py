#!/usr/bin/env python
# coding: utf-8
#D.McKay Jun 2024


import sys
import os
from multiprocessing import Pool
from multiprocessing import cpu_count
from itertools import repeat
from functools import partial
from typing import List
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

def find_metadata(key: str, bucket) -> List[str]:
    """
    Finds the metadata for a given key in an S3 bucket.

    Args:
        key (dd.core.Scalar or str): The key to search for metadata.
        s3: The S3 object.

    Returns:
        list[str]: A list of existing metadata contents if found, otherwise empty list.
    """
    if type(key) == str:
        existing_zip_contents = None
        if key.endswith('.zip'):
            try:
                existing_zip_contents = str(bucket.Object(''.join([key,'.metadata'])).get()['Body'].read().decode('UTF-8')).split('|') # use | as separator
            except Exception as e:
                try:
                    existing_zip_contents = bucket.Object(key).metadata['zip-contents'].split('|') # use | as separator
                except KeyError:
                    return [None]
            if existing_zip_contents:
                return existing_zip_contents
        else:
            return [None]
    else:
        return [None]

def get_zipfile_lists(bucket_name, access_key, secret_key, s3_host, get_contents_metadata, debug):
    zipfile_list = []
    contents_list = []
    zipfile_sizes = []
    contents_objects = []
    s3 = bm.get_resource(access_key, secret_key, s3_host)
    s3_client = bm.get_client(access_key, secret_key, s3_host)
    bucket = s3.Bucket(bucket_name)

    pattern = re.compile(r'.*collated_\d+\.zip$')

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name)
    zipfile_count = 0
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if pattern.match(key):
                    zipfile_count += 1
                    zipfile_list.append(key)
                    if get_contents_metadata:
                        try:
                            metadata = bucket.Object(key).get()['Metadata']
                            if 'zip-contents' in metadata:
                                print('Using zip-contents metadata.')
                                contents_list.append(find_metadata(key, bucket))
                                contents_objects.append(False)
                            elif 'zip-contents-object' in metadata:
                                print('Using zip-contents-object.')
                                contents = metadata['zip-contents-object']
                                contents_list.append(find_metadata(contents, bucket))
                                contents_objects.append(True)
                            else:
                                raise KeyError(f'Key {key} has no zip-contents metadata.')

                        except KeyError as e:
                            print(f'Key {key} has no zip-contents metadata.')
                            contents_list.append([])
                        zipfile_sizes.append(obj['Size'])
            print(f'Zip files found: {zipfile_count}', end='\r')
            # for debugging
            if debug:
                if zipfile_count >= 200:
                    break
    print()
    a = np.array([zipfile_list,contents_objects,contents_list], dtype=object).T
    print(a.shape)
    zipfile_df = pd.DataFrame(a, columns=['zipfile','contents_object','contents'])
    if debug:
        print(zipfile_df)
    return zipfile_df
    
s3_host = 'echo.stfc.ac.uk'
try:
    keys = bm.get_keys()
except KeyError as e:
    print(e)
    sys.exit()
access_key = keys['access_key']
secret_key = keys['secret_key']

import warnings
warnings.filterwarnings('ignore')

s3 = bm.get_resource(access_key,secret_key,s3_host)
get_contents_metadata = True
debug = True
bucket_name = 'LSST-IR-FUSION-rdsip005'

zipfiles_df = get_zipfile_lists(bucket_name, access_key, secret_key, s3_host, get_contents_metadata, debug)

print(zipfiles_df)