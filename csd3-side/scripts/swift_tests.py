#!/usr/bin/env python
# coding: utf-8

import sys
import bucket_manager.bucket_manager as bm
import numpy as np
import os
import json
s3_host = 'https://s3.echo.stfc.ac.uk/auth/1.0'

# keys = bm.get_keys(api)
with open(os.path.expanduser('~/lsst-swift-credentials.json'), 'r') as kf:
    keys = json.load(kf)
user = keys['user']
secret_key = keys['secret_key']

swift = bm.get_conn_swift(user, secret_key, s3_host)
print(swift)

swift_service = bm.get_service_swift(user, secret_key, s3_host)
print(swift_service)

bucket_list = bm.bucket_list_swift(swift)

print(bucket_list)

bucket_name = 'test-large-file'

large_file_path = sys.argv[1]
if large_file_path.startswith('~'):
    large_file_path = os.path.expanduser(large_file_path)
elif not os.path.isabs(large_file_path):
    large_file_path = os.path.abspath(large_file_path)

try:
    current_objects = bm.object_list_swift(swift, bucket_name, full_listing=False)
except Exception as e:
    print(f'Error: {e}')

print(current_objects)

try:
    swift.put_container(bucket_name)
except Exception as e:
    print(f'Error: {e}')


print(f'len(current_objects): {len(current_objects)}')

'''
```python
#swift api note
container = 'new-container'
    with open('local.txt', 'r') as local:
        conn.put_object(
            container,
            'local_object.txt',
            contents=local,
            content_type='text/plain'
        )
```
'''
segment_size = 2*1024**3 # 2GiB

with open(large_file_path, 'rb') as lf:
    file_size = len(lf.read())
    n_segments = int(np.ceil(file_size / segment_size))
    print(n_segments)
    print(file_size)
    lf.seek(0)
    segments = []
    for i in range(n_segments):
        start = i * segment_size
        end = min(start + segment_size, file_size)
        print(start, end)
        segment_number = i + 1
        print(segment_number)
        segments.append(lf.read()[start:end])
        print(len(segments))
        segment_objects = [ bm.get_SwiftUploadObject(bucket_name, f'{large_file_path}_segmented_{segment_number}', options={'contents':segment, 'content_type':'bytes'}) for segment_number, segment in enumerate(segments) ]
        print(segment_objects)

swift_service.upload(
    bucket_name,
    segment_objects,
    options={
            'meta': [],
            'header': [],
            'segment_size': segment_size,
            'use_slo': True,
            'segment_container': bucket_name,
            'leave_segments': False,
            'changed': None,
            'skip_identical': False,
            'skip_container_put': False,
            'fail_fast': True,
            'dir_marker': False  # Only for None sources
        }
    )

