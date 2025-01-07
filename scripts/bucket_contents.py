#!/usr/bin/env python
# coding: utf-8
# D. McKay Feb 2024

import argparse
import sys
import os
import bucket_manager.bucket_manager as bm

def list_all(bucket):
    total_size = 0
    for ob in bucket.objects.all():
        if ob.key.endswith('zip'):
            try:
                print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, {s3.ObjectSummary(bucket_name, ob.key).get()["Metadata"]["zip-contents"]}')
            except Exception as e:
                print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, n/a')
        else:
            print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, n/a')
        total_size += ob.size
    return total_size

def list_prefix(bucket,prefix):
    total_size = 0
    for ob in bucket.objects.filter(Prefix=prefix):
        if ob.key.endswith('zip'):
            try:
                print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, {s3.ObjectSummary(bucket_name, ob.key).get()["Metadata"]["zip-contents"]}')
            except Exception as e:
                print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, n/a')
        else:
            print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, n/a')
        total_size += ob.size
    return total_size

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Measure transfer speed to an S3 bucket.')
    parser.add_argument('--bucket-name', '-b', type=str, help='Name of the S3 bucket', required=True)
    parser.add_argument('--prefix', '-p', type=str, default='', help='Optional prefix to filter objects in the bucket')

    args = parser.parse_args()
    bucket_name = args.bucket_name
    prefix = args.prefix

    try:
        assert bm.check_keys()
    except AssertionError as e:
        print(e)
        sys.exit()

    import warnings
    warnings.filterwarnings('ignore')

    s3 = bm.get_resource()

    bucket = s3.Bucket(bucket_name)

    try:
        print('Contents of bucket: ',bucket_name)
        print('Object, Size / MiB, LastModified, Zip Contents')
        if prefix:
            total_size = list_prefix(bucket,prefix)
        else:
            total_size = list_all(bucket)
    except Exception as e:
        if '(NoSuchBucket)' in str(e).split():
            print('NoSuchBucket')

    print(f'Total size of bucket: {total_size/1024**3:.2f} GiB')