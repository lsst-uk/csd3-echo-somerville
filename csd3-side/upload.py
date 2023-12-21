import sys
import os
import re
from multiprocessing import Pool
from itertools import repeat
import bucket_manager
from datetime import datetime
import hashlib
import pandas as pd
import numpy as np

def upload_to_bucket(bucket,folder,filename,destination_key,perform_checksum):
	if type(bucket) is not str:
		bucket_name = bucket.name
	else:
		bucket_name = bucket
	if perform_checksum:
		file_data = open(filename, 'rb').read()
		checksum = hashlib.md5(file_data).hexdigest().encode('utf-8')
		if upload_checksum and not dryrun:
			checksum_key = destination_key + '.checksum'
			#create checksum object
			key = bucket.new_key(checksum_key)
			key.set_contents_from_string(checksum)
	"""
	- Upload the file to the bucket
	"""
	if not dryrun:
		key = bucket.new_key(destination_key)
		key.set_contents_from_filename(filename)

	"""
		report actions
		CSV formatted
		header: LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,CHECKSUM_SIZE,CHECKSUM_KEY
	"""
	return_string = f'{folder},{filename},{os.stat(filename).st_size},{bucket_name},{destination_key}'
	if perform_checksum and upload_checksum:
		return_string += f',{checksum},{len(checksum)},{checksum_key}'
	elif perform_checksum:
		return_string += f',{checksum},,'
	else:
		return_string += ',,,'
	return return_string

def process_files(bucket, source_dir, destination_dir, ncores, perform_checksum, log):
	i = 0
	#processed_files = []
	with Pool(ncores) as pool: # use 4 CPUs by default
		#recursive loop over local folder
		for folder,subfolders,files in os.walk(source_dir):
			# check folder isn't empty
			if len(files) > 0:
				# all files within folder
				folder_files = [ os.sep.join([folder,filename]) for filename in files ]
				# keys to files on s3
				destination_keys = [ os.sep.join([destination_dir, os.path.relpath(filename, source_dir)]) for filename in folder_files ]
				folder_start = datetime.now()
				file_count = len(files)
				# upload files in parallel and log output
				print(f'Uploading {file_count} files from {folder} using {ncores} processes.')
				with open(log, 'a') as logfile:
					for result in pool.starmap(upload_to_bucket, zip(repeat(bucket), repeat(folder), folder_files, destination_keys, repeat(perform_checksum))):
						logfile.write(f'{result}\n')
				folder_end = datetime.now()
				folder_files_size = np.sum(np.array([os.path.getsize(filename) for filename in folder_files]))
				print_stats(log, folder, file_count, folder_files_size, folder_start, folder_end)

				# testing - stop after 10 folders
				i+=1
				if i == 10:
					break
	# Upload log file
	if not dryrun:
		upload_to_bucket(bucket, '/', log, os.path.basename(log), False)

def print_stats(log,folder,file_count,total_size,folder_start,folder_end):
	elapsed = folder_end - folder_start
	print(f'Finished folder {folder}, elapsed time = {elapsed}')
	elapsed_seconds = elapsed.seconds + elapsed.microseconds / 1e6
	avg_file_size = total_size / file_count / 1024**2
	if not upload_checksum:
		print(f'{file_count} files (avg {avg_file_size:.2f} MiB/file) uploaded in {elapsed_seconds:.2f} seconds, {file_count / elapsed_seconds:.2f} files/sec')
		print(f'{total_size / 1024**2:.2f} bytes uploaded in {elapsed_seconds:.2f} seconds, {total_size / 1024**2 / elapsed_seconds:.2f} MiB/sec')
	if upload_checksum:
		checksum_size = 32*file_count # checksum byte strings are 32 bytes
		total_size += checksum_size
		file_count *= 2
		print(f'{file_count} files (avg {avg_file_size:.2f} MiB/file) uploaded (including checksum files) in {elapsed_seconds:.2f} seconds, {file_count / elapsed_seconds:.2f} files/sec')
		print(f'{total_size / 1024**2:.2f} bytes uploaded (including checksum files) in {elapsed_seconds:.2f} seconds, {total_size / 1024**2 / elapsed_seconds:.2f} MiB/sec')



if __name__ == '__main__':
	# Initiate timing
	start = datetime.now()
	# Set the source directory, bucket name, and destination directory
	source_dir = "/rds/project/rds-rPTGgs6He74/data/private/VISTA/VIDEO/"
	log = f"{'-'.join(source_dir.split('/')[-2:])}-files.csv"
	destination_dir = "VIDEO"
	folders = []
	folder_files = []
	ncores = 4 # change to adjust number of CPUs (= number of concurrent connections)
	perform_checksum = True
	upload_checksum = False
	dryrun = False

	# Add titles to log file
	with open(log, 'w') as logfile: # elsewhere open(log, 'a')
		logfile.write('LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,CHECKSUM_SIZE,CHECKSUM_KEY\n')
	
	# Setup bucket
	s3_host = 'echo.stfc.ac.uk'
	keys = bucket_manager.get_keys(os.sep.join([os.environ['HOME'],'lsst_keys.json']))
	access_key = keys['access_key']
	secret_key = keys['secret_key']
	
	conn = bucket_manager.get_conn(access_key, secret_key, s3_host)
	
	bucket_name = 'dm-test'
	if dryrun:
		mybucket = 'dummy_bucket'
	
	if bucket_name not in [bucket.name for bucket in conn.get_all_buckets()]:
		if not dryrun:
        		mybucket = conn.create_bucket(bucket_name)
        		print(f'Added bucket: {bucket_name}')
	else:
		if not dryrun:
			print(f'Bucket exists: {bucket_name}')
			sys.exit('Bucket exists.')
		else:
			print(f'Bucket exists: {bucket_name}')
			print('dryrun = True, so continuing.')
	
	# Process the files in parallel
	print(f'Starting processing at {datetime.now()}, elapsed time = {datetime.now() - start}')
	process_files(mybucket, source_dir, destination_dir, ncores, perform_checksum, log)

	# Complete
	print(f'Finished at {datetime.now()}, elapsed time = {datetime.now() - start}')
