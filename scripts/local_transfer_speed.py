#!/usr/bin/env python
# coding: utf-8
# D. McKay May 2024

import os
from datetime import datetime
import time
import sys

path = sys.argv[1]
files = []
start_num_files = 0
start_size = 0
for root, dirs, files in os.walk(path):
    start_num_files += len(files)
    for file in files:
        start_size += os.path.getsize(os.path.join(root, file))

start_time = datetime.now()  # Move the start_time assignment here

print(f'Starting number of files = {start_num_files}, starting size = {start_size}')

for i in range(5):
    print(i)
    time.sleep(1)

end_num_files = 0
end_size = 0
for root, dirs, files in os.walk(path):
    end_num_files += len(files)
    for file in files:
        end_size += os.path.getsize(os.path.join(root, file))

end_time = datetime.now()  # Move the end_time assignment here

print(f'Ending number of files = {end_num_files}, ending size = {end_size}')

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
