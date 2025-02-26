#!/usr/bin/env python
# coding: utf-8

import sys
import bucket_manager.bucket_manager as bm
import numpy as np
import os
import json



# s3_host = os.environ['ST_AUTH']
# user = os.environ['ST_USER']
# secret_key = os.environ['ST_KEY']
if not bm.check_keys('swift'):
    sys.exit('Error: Missing environment variables for Swift API.')
swift = bm.get_conn_swift()
print(swift)

swift_service = bm.get_service_swift()
print(swift_service)

bucket_list = bm.bucket_list_swift(swift)

print(bucket_list)

bucket_name = 'test-large-file'
try:
    results = swift.put_container(bucket_name)
    if results is not None:
        print([result for result in results])
except Exception as e:
    print(f'Error: {e}')


large_file_path = sys.argv[1]
if large_file_path.startswith('~'):
    large_file_path = os.path.expanduser(large_file_path)
elif not os.path.isabs(large_file_path):
    large_file_path = os.path.abspath(large_file_path)

current_objects = None
try:
    current_objects = bm.object_list_swift(swift, bucket_name, full_listing=False)
except Exception as e:
    print(f'Error: {e}')

print(current_objects)


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
prefix = 'test'
remote_path = f'{prefix}/{os.path.basename(large_file_path)}'
print(f'remote_path {remote_path}')
segments = []

with open(large_file_path, 'rb') as lf:
    file_size = len(lf.read())
    n_segments = int(np.ceil(file_size / segment_size))
    print(n_segments)
    print(file_size)
    lf.seek(0)
    for i in range(n_segments):
        start = i * segment_size
        end = min(start + segment_size, file_size)
        print(start, end)
        segment_number = i + 1
        print(segment_number)
        segments.append(lf.read()[start:end])
        print(len(segments))
segment_objects = [ bm.get_SwiftUploadObject(large_file_path, object_name=remote_path, options={'contents':segment, 'content_type':'bytes'}) for segment in segments ]
print(segment_objects)
segmented_upload = [large_file_path]
for so in segment_objects:
    segmented_upload.append(so)

print(f'Uploading large file in {len(segment_objects)} segments.')
results = swift_service.upload(
    bucket_name,
    segmented_upload,
    options={
        'meta': [],
        'header': [],
        'segment_size': segment_size,
        'use_slo': True,
        'segment_container': bucket_name+'-segments',
        'leave_segments': False,
        'changed': None,
        'skip_identical': False,
        'skip_container_put': False,
        'fail_fast': False,
        'dir_marker': False 
        }
    )

for result in results:
    if result['action'] == 'upload_object':
        print(result)