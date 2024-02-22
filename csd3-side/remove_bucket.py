import boto
import json
import os
import boto.s3.connection
import bucket_manager

s3_host = 'echo.stfc.ac.uk'
keys = bucket_manager.get_keys(os.sep.join([os.environ['HOME'],'lsst_keys.json']))
access_key = keys['access_key']
secret_key = keys['secret_key']

conn = bucket_manager.get_conn(access_key, secret_key, s3_host)
bucket = conn.get_bucket('csd3-backup-test')

for key in bucket.list():
	print("{name}\t{size}\t{modified}".format(
		name = key.name,
		size = key.size,
		modified = key.last_modified,
	))

for key in bucket.list():
	print(f'Deleting {key}')
	bucket.delete_key(key)
print(f'Deleting bucket {bucket.name}')
conn.delete_bucket(bucket.name)
