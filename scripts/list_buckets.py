#!/usr/bin/env python
# coding: utf-8
# D. McKay Feb 2024
import sys, os
import bucket_manager.bucket_manager as bm

try:
    assert bm.check_keys()
except AssertionError as e:
    print(e)
    sys.exit()

import warnings
warnings.filterwarnings('ignore')

s3 = bm.get_resource()

print(bm.bucket_list(s3))