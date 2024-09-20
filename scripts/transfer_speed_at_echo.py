#!/usr/bin/env python
# coding: utf-8
# D. McKay March 2024

import os
from datetime import datetime
import time
import sys

import bucket_manager.bucket_manager as bm
import warnings
with warnings.catch_warnings():
    warnings.filterwarnings('ignore')

    s3_host = 'echo.stfc.ac.uk'
    try:
        keys = bm.get_keys()
    except KeyError as e:
        print(e)
        sys.exit()

    access_key = keys['access_key']
    secret_key = keys['secret_key']

    s3 = bm.get_resource(access_key, secret_key, s3_host)

    bucket_name = sys.argv[1]
    bucket = s3.Bucket(bucket_name)
    prefix = ''
    if len(sys.argv) == 3:
        prefix = sys.argv[2]

    print('Connection established.')

    start_num_files = 0
    start_size = 0

    start_time = datetime.now()  # Move the start_time assignment here

    for ob in bucket.objects.all(): #filter(Prefix = prefix):
        start_num_files += 1
        start_size += ob.size
        #print(ob.size)

    print(start_size)

    for i in range(5):
        print(i)
        time.sleep(1)

    end_num_files = 0
    end_size = 0
    for ob in bucket.objects.all(): #filter(Prefix = prefix):
        end_num_files+=1
        end_size += ob.size
    end_time = datetime.now()
    print(end_size)

    elapsed = end_time - start_time
    elapsed_seconds = elapsed.seconds + elapsed.microseconds / 1e6
    size_diff = end_size - start_size
    size_diff_MiB = size_diff / 1024**2
    files_diff = end_num_files - start_num_files

    transfer_speed = size_diff_MiB / elapsed_seconds
    if files_diff > 0:
        seconds_per_file = elapsed_seconds / files_diff
    else:
        seconds_per_file = 0

    print(f'Transfer speed = {transfer_speed:.2f} MiB/s - {seconds_per_file:.2f} s/file')
