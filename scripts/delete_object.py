#!/usr/bin/env python
# coding: utf-8
# D. McKay Feb 2024

"""
This script allows you to delete a single file given its bucket name and URI.
"""

import sys
import warnings

import bucket_manager.bucket_manager as bm

s3_host = 'echo.stfc.ac.uk'
try:
    keys = bm.get_keys()
except KeyError as e:
    print(e)
    sys.exit()
access_key = keys['access_key']
secret_key = keys['secret_key']

warnings.filterwarnings('ignore')

s3 = bm.get_resource(access_key, secret_key, s3_host)

bucket_name = sys.argv[1]
URI = sys.argv[2]

if not bucket_name in bm.bucket_list(s3):
    sys.exit('Bucket does not exist.')

bucket = s3.Bucket(bucket_name)

if not URI in bm.object_list(bucket):
    sys.exit('Object does not exist.')

sure = input("Are you sure? [y/n]\n").lower()

if sure == 'n':
    sys.exit('Aborted.')
elif sure == 'y':
    pass
else:
    sys.exit('Aborted.')
try:
    response = s3.Object(bucket_name, URI).delete()
except Exception as e:
    print(e)


# Confirm
if response['ResponseMetadata']['HTTPStatusCode'] == 204 and not URI in bm.object_list(bucket):
    print(f'Object {URI} deleted.')