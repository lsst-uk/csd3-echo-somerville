#!/usr/bin/env python
# coding: utf-8
# D. McKay Feb 2024

"""
This script allows you to upload a single file given bucket name, object_name and local path.
"""

import sys
import os
import warnings
import argparse
import datetime as dt

import bucket_manager.bucket_manager as bm


def upload_file(connection, bucket_name, object_name, local_path, timings=False):
    """
    Upload a file to an S3 bucket.
    Optionally print timings for data loading and upload.
    """
    if timings:
        read_start = dt.datetime.now()
        file_data = open(local_path, 'rb').read()
        read_end = dt.datetime.now()
        size = os.path.getsize(local_path)
    else:
        file_data = open(local_path, 'rb').read()
    try:
        if timings:
            upload_start = dt.datetime.now()
        response = connection.put_object(
            bucket=bucket_name,
            obj=object_name,
            contents=file_data
        )
        if timings:
            upload_end = dt.datetime.now()
    except Exception as e:
        print(f'Error {e}', file=sys.stderr)
        sys.exit()
    if timings:
        end = dt.datetime.now()
        print(f'File size: {size} bytes.')
        print(f'Read time: {(read_end - read_start).total_seconds()} seconds.')
        print(f'Upload time: {(upload_end - upload_start).total_seconds()} seconds.')
        print(f'Transfer speed: {size / (upload_end - upload_start).total_seconds()} bytes per second.')
        print(f'Total time: {(end - read_start).total_seconds()} seconds.')
    return response


parser = argparse.ArgumentParser(description='Upload a file to an S3 bucket.')
parser.add_argument('--bucket-name', '-b', type=str, help='The name of the S3 bucket.')
parser.add_argument('--object-name', '-o', type=str, help='The name of the object to upload.')
parser.add_argument('--local-path', '-p', type=str, help='The local path to the file to upload.')
parser.add_argument('--timings', '-t', action='store_true', help='Timings for data loading and upload.')
parser.add_argument('--api', type=str, help='The API to use for the upload.')

args = parser.parse_args()

bucket_name = args.bucket_name
object_name = args.object_name
local_path = args.local_path
timings = args.timings
api = args.api.lower()

if api == 's3':
    sys.exit('S3 API not yet implemented.')
elif api == 'swift':
    pass
else:
    sys.exit('API not recognised.')

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

warnings.filterwarnings('ignore')

connection = bm.get_conn_swift(s3_host, access_key, secret_key)

if bucket_name not in bm.bucket_list_swift(connection):
    sys.exit('Bucket does not exist.')

response = upload_file(
    connection,
    bucket_name,
    object_name,
    local_path,
    timings
)
