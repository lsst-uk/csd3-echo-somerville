#!/usr/bin/env python
# coding: utf-8
# D. McKay Feb 2024
import sys, os
import bucket_manager.bucket_manager as bm


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

print(bm.bucket_list(s3))