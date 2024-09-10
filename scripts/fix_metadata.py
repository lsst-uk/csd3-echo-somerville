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
                existing_zip_contents_str = str(bucket.Object(''.join([key,'.metadata'])).get()['Body'].read().decode('UTF-8'))
            except Exception as e:
                try:
                    existing_zip_contents_str = bucket.Object(key).metadata['zip-contents']
                except KeyError:
                    return None
            if existing_zip_contents:
                # Guess separator
                if '|' in existing_zip_contents:
                    existing_zip_contents = existing_zip_contents.split('|')
                elif ';' in existing_zip_contents:
                    existing_zip_contents = existing_zip_contents.split(';')
                else:
                    existing_zip_contents = existing_zip_contents.split(',')
                return existing_zip_contents
        else:
            return None
    else:
        return None

def get_zipfile_lists(bucket_name, access_key, secret_key, s3_host, debug):
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
    contents_count = 0
    contents_object_count = 0
    no_metadata_count = 0
    object_count = 0
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                object_count += 1
                key = obj['Key']
                if pattern.match(key):
                    zipfile_count += 1
                    zipfile_list.append(key)
                    try:
                        metadata = bucket.Object(key).get()['Metadata']
                        if 'zip-contents' in metadata:
                            contents_count += 1
                            contents_list.append(find_metadata(key, bucket))
                            contents_objects.append(False)
                            # print('Using zip-contents metadata.')
                        elif 'zip-contents-object' in metadata:
                            contents_object_count += 1
                            contents = metadata['zip-contents-object']
                            contents_list.append(find_metadata(contents, bucket))
                            contents_objects.append(True)
                            # print('Using zip-contents-object.')
                        else:
                            no_metadata_count += 1
                            contents_list.append(None)
                            contents_objects.append(False)
                            # print(f'Key {key} has no zip-contents metadata.')
                            raise KeyError(f'Key {key} has no zip-contents metadata.')

                    except KeyError as e:
                        print(f'Key {key} has no zip-contents metadata.')
                        
                    zipfile_sizes.append(obj['Size'])
            print(f'Objects found: {object_count}, zip files found: {zipfile_count}. {contents_count} with metadata value, {contents_object_count} with metadata object, {no_metadata_count} with no metadata.', end='\r')
            # for debugging
            if debug:
                if zipfile_count >= 200:
                    break
    print()
    print(len(zipfile_list),len(contents_list),len(contents_objects))
    zipfile_df = pd.DataFrame(np.array([zipfile_list,contents_objects,contents_list], dtype=object).T, columns=['zipfile','contents_object','contents'])
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
debug = True
bucket_name = 'LSST-IR-FUSION-Butlers'

zipfiles_df = get_zipfile_lists(bucket_name, access_key, secret_key, s3_host, debug)

print(zipfiles_df)