import boto
import json
import os
import boto.s3.connection
from datetime import datetime
import time

def print_buckets(conn):
    for bucket in conn.get_all_buckets():
            print("{name}\t{created}".format(name = bucket.name,created = bucket.creation_date))
def get_keys(json_file):
    with open(json_file, 'r') as keyfile:
        keys = json.load(keyfile)
    return keys

def get_conn(access_key, secret_key, host):
    return boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = host,
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )


s3_host = 'echo.stfc.ac.uk'
keys = get_keys(os.sep.join([os.environ['HOME'],'lsst_keys.json']))

access_key = keys['access_key']
secret_key = keys['secret_key']

conn = get_conn(access_key, secret_key, s3_host)

bucket = conn.get_bucket('dm-test')

print('Connection established.')

start_num_files = 0
start_size = 0
for key in bucket.list():
	start_num_files+=1
	start_size+=key.size
start_time = datetime.now()
print(start_size)
for i in range(5):
	print(i)
	time.sleep(1)
end_num_files = 0
end_size = 0
for key in bucket.list():
        end_num_files+=1
        end_size+=key.size
end_time = datetime.now()
print(end_size)

elapsed = end_time - start_time
elapsed_seconds = elapsed.seconds + elapsed.microseconds / 1e6
size_diff = end_size - start_size
size_diff_MiB = size_diff / 1024**2
files_diff = end_num_files - start_num_files

transfer_speed = size_diff_MiB / elapsed_seconds
if files_diff > 0:
	seconds_per_file = elapsed_seconds / files_diff
else:
	seconds_per_file = 0

print(f'Transfer speed = {transfer_speed:.2f} MiB/s - {seconds_per_file:.2f} s/file')
