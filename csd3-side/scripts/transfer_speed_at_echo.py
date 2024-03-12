import os
from datetime import datetime
import time
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import bucket_manager.bucket_manager as bm



s3_host = 'echo.stfc.ac.uk'
keys = bm.get_keys(api='S3')

access_key = keys['access_key']
secret_key = keys['secret_key']

s3 = bm.get_resource(access_key, secret_key, s3_host)

bucket_name = sys.argv[1]
bucket = s3.Bucket(bucket_name)

print('Connection established.')

start_num_files = 0
start_size = 0
for ob in bucket.objects.all():
	start_num_files+=1
	start_size+=ob.size
start_time = datetime.now()
print(start_size)
for i in range(5):
	print(i)
	time.sleep(1)
end_num_files = 0
end_size = 0
for ob in bucket.objects.all():
	start_num_files+=1
	start_size+=ob.size
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
