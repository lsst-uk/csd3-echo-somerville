#!/usr/bin/env python
# coding: utf-8
# D. McKay Feb 2024

import argparse
import sys
import os
import bucket_manager.bucket_manager as bm

def list_all(bucket,limit,names_to_json):
    total_size = 0
    count = 0
    if names_to_json:
        jfile = open('/airflow/xcom/return.json','a')
    for ob in bucket.objects.all():
        if ob.key.endswith('zip'):
            try:
                if names_to_json:
                    print(f'"{ob.key}"')
                    jfile.write(json.dumps(ob.key + "\n"))
                else:
                    print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, {s3.ObjectSummary(bucket_name, ob.key).get()["Metadata"]["zip-contents"]}')
            except Exception as e:
                if names_to_json:
                    print(f'"{ob.key}"')
                    jfile.write(json.dumps(ob.key + "\n"))
                else:
                    print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, n/a')
        else:
            if names_to_json:
                print(f'"{ob.key}"')
                jfile.write(json.dumps(ob.key + "\n"))
            else:
                print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, n/a')
        total_size += ob.size
        count += 1
        if count == limit:
            break
    if names_to_json:
        jfile.close()
    return total_size

def list_prefix(bucket,prefix,limit,names_to_json):
    total_size = 0
    count = 0
    if names_to_json:
        jfile = open('/airflow/xcom/return.json','a')
    for ob in bucket.objects.filter(Prefix=prefix):
        if ob.key.endswith('zip'):
            try:
                if names_to_json:
                    print(f'"{ob.key}"')
                    jfile.write(json.dumps(ob.key + "\n"))
                else:
                    print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, {s3.ObjectSummary(bucket_name, ob.key).get()["Metadata"]["zip-contents"]}')
            except Exception as e:
                if names_to_json:
                    print(f'"{ob.key}"')
                    jfile.write(json.dumps(ob.key + "\n"))
                else:
                    print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, n/a')
        else:
            if names_to_json:
                print(f'"{ob.key}"')
                jfile.write(json.dumps(ob.key + "\n"))
            else:
                print(f'{ob.key}, {ob.size/1024**2:.2f}, {ob.last_modified}, n/a')
        total_size += ob.size
        count += 1
        if count == limit:
            break
    if names_to_json:
        jfile.close()
    return total_size

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Measure transfer speed to an S3 bucket.')
    parser.add_argument('--bucket-name', '-b', type=str, help='Name of the S3 bucket', required=True)
    parser.add_argument('--prefix', '-p', type=str, default='', help='Optional prefix to filter objects in the bucket')
    parser.add_argument('--limit', '-l', type=int, default=0, help='Optional limit on the number of objects to list')
    parser.add_argument('--names-to-json', '-n', action='store_true', default=False, help='Only list the names of the objects')

    args = parser.parse_args()
    bucket_name = args.bucket_name
    prefix = args.prefix
    limit = args.limit
    names_to_json = args.names_to_json

    if names_to_json:
        import json

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
        if names_to_json:
            print("Names only")
        else:
            print('Object, Size / MiB, LastModified, Zip Contents')
        if prefix:
            total_size = list_prefix(bucket,prefix,limit,names_to_json)
        else:
            total_size = list_all(bucket,limit,names_to_json)
    except Exception as e:
        if '(NoSuchBucket)' in str(e).split():
            print('NoSuchBucket')
    if names_to_json:
        print('Names written to /airflow/xcom/return.json')
        os.chmod('/airflow/xcom/return.json',0o444)
    print(f'Total size of bucket: {total_size/1024**3:.2f} GiB')