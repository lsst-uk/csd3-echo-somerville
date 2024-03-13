#!/usr/bin/env python
# coding: utf-8
# D. McKay Feb 2024
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import bucket_manager.bucket_manager as bm

s3_host = 'echo.stfc.ac.uk'
keys = bm.get_keys()
access_key = keys['access_key']
secret_key = keys['secret_key']

import warnings
warnings.filterwarnings('ignore')

s3 = bm.get_resource(access_key,secret_key,s3_host)

print(bm.bucket_list(s3))