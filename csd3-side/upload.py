import sys
import os
import re
from multiprocessing import Pool
from itertools import repeat
import bucket_manager
from datetime import datetime
import hashlib
import pandas as pd

def upload_to_bucket(bucket,filename,destination_key,perform_checksum):
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
		header: LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,CHECKSUM_SIZE,CHECKSUM_KEY
	"""
	return_string = f'{filename},{os.stat(filename).st_size},{bucket_name},{destination_key}'
	if perform_checksum:
		return_string += f',{checksum}'
	else:
		 return_string += ','
	if upload_checksum:
		return_string += f',{len(checksum)},{checksum_key}'
	else:
		return_string += ',,'
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
				
				# upload files in parallel and log output
				print(f'Uploading {len(files)} files from {folder} using {ncores} processes.')
				with open(log, 'a') as logfile:
					for result in pool.starmap(upload_to_bucket, zip(repeat(bucket), folder_files, destination_keys, repeat(perform_checksum))):
						logfile.write(f'{result}\n')

				# testing - stop after 1 folders
				i+=1
				if i == 1:
					break
	# Upload log file
	if not dryrun:
		upload_to_bucket(bucket, log, os.path.basename(log), False)

if __name__ == '__main__':
	# Initiate timing
	start = datetime.now()
	# Set the source directory, bucket name, and destination directory
	source_dir = "/rds/project/rds-rPTGgs6He74/data/private/VISTA/VIDEO/"
	log = f"{'-'.join(source_dir.split('/')[-2:])}_files.csv"
	destination_dir = "VIDEO"
	folders = []
	folder_files = []
	ncores = 4 # change to adjust number of CPUs (= number of concurrent connections)
	perform_checksum = True
	upload_checksum = False
	dryrun = True

	# Add titles to log file
	with open(log, 'w') as logfile: # elsewhere open(log, 'a')
		logfile.write('LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,CHECKSUM_SIZE,CHECKSUM_KEY\n')
	
	# Setup bucket
	s3_host = 'echo.stfc.ac.uk'
	keys = bucket_manager.get_keys(os.sep.join([os.environ['HOME'],'lsst_keys.json']))
	access_key = keys['access_key']
	secret_key = keys['secret_key']
	
	conn = bucket_manager.get_conn(access_key, secret_key, s3_host)
	
	bucket_name = 'dm-test'
	
	if bucket_name not in [bucket.name for bucket in conn.get_all_buckets()]:
		if not dryrun:
        		mybucket = conn.create_bucket(bucket_name)
        		print(f'Added bucket: {bucket_name}')
		else:
			mybucket = 'dummy_bucket'
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
	total_time = datetime.now() - start
	print(f'Finished at {datetime.now()}, elapsed time = {total_time}')
	log_results = pd.read_csv(log)
	file_count = len(log_results)
	total_size = sum(log_results['FILESIZE'])
	print(f'{file_count} files uploaded in {total_time.seconds} seconds, {file_count / (total_time.seconds + 1)} files/sec')
	print(f'{total_size} bytes uploaded in {total_time.seconds} seconds, {total_size / 1024**2 / (total_time.seconds + 1)} MiB/sec')
	if upload_checksum:
		checksum_size = sum(log_results['CHECKSUM_SIZE'])
		total_size += checksum_size
		file_count *= 2
		print(f'{file_count} files uploaded (including checksum files) in {total_time.seconds} seconds, {file_count / (total_time.seconds + 1)} files/sec')
		print(f'{total_size} bytes uploaded (including checksum files) in {total_time.seconds} seconds, {total_size / 1024**2 / (total_time.seconds + 1)} MiB/sec')

