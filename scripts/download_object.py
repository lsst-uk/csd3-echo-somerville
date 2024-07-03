#!/usr/bin/env python
# coding: utf-8
# D. McKay Feb 2024

"""
This script allows you to download a single file to the local directory given its bucket name and URI.
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

# if not URI in bm.object_list(bucket):
#     sys.exit('Object does not exist.')


bucket.download_file(URI, URI.split('/')[-1]) 