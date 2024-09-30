#!/usr/bin/env python
# coding: utf-8
# D. McKay March 2024

import os
from datetime import datetime
import time
import sys

import bucket_manager.bucket_manager as bm
import warnings
import argparse

def size_all(bucket):
    num_files = 0
    size = 0
    for ob in bucket.objects.all():
        num_files += 1
        size += ob.size
    return num_files, size

def size_prefix(bucket,prefix):
    num_files = 0
    size = 0
    for ob in bucket.objects.filter(Prefix = prefix):
        num_files += 1
        size += ob.size
    return num_files, size

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Measure transfer speed to an S3 bucket.')
    parser.add_argument('--bucket-name', '-b', type=str, help='Name of the S3 bucket', required=True)
    parser.add_argument('--prefix', '-p', type=str, default='', help='Optional prefix to filter objects in the bucket')

    args = parser.parse_args()
    bucket_name = args.bucket_name
    prefix = args.prefix

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

        bucket = s3.Bucket(bucket_name)

        print('Connection established.')

        start_time = datetime.now()  # Move the start_time assignment here
        if prefix:
            start_num_files, start_size = size_prefix(bucket, prefix)
        else:
            start_num_files, start_size = size_all(bucket)

        print(start_size)

        for i in range(5):
            print(i)
            time.sleep(1)

        if prefix:
            end_num_files, end_size = size_prefix(bucket, prefix)
        else:
            end_num_files, end_size = size_all(bucket)
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
