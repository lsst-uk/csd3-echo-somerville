import os
import re
from multiprocessing import Pool
from itertools import repeat
import bucket_manager
from datetime import datetime
import hashlib

def dryrun_upload_to_bucket(bucket_name,filename,destination_key,perform_checksum):
	if perform_checksum:
		checksum_key = destination_key + '.checksum'
		file_data = open(filename, 'rb').read()
		checksum = hashlib.md5(file_data).hexdigest().encode('utf-8')
	# Upload the file to the bucket
        #s3.Object(bucket_name, destination_key).upload_file(filename)
	#with open(log, 'a') as logfile:
	#	logfile.write(f'--dryrun-- local: {filename}, {os.stat(filename).st_size} remote ({bucket_name}): {destination_key}\n')
	return_string = f'--dryrun-- local: {filename}, {os.stat(filename).st_size}, remote ({bucket_name}): {destination_key}'
	if perform_checksum:
		return_string += f'\n--dryrun-- checksum: {checksum}, {len(checksum)}, remote ({bucket_name}): {checksum_key}'

	return return_string

def upload_to_bucket(bucket_name,filename,destination_key,perform_checksum):
	if perform_checksum:
                checksum_key = destination_key + '.checksum'
                file_data = open(filename, 'rb').read()
                checksum = hashlib.md5(file_data).hexdigest().encode('utf-8')
	"""
	- Upload the file to the bucket
	"""
	#example from file
	#key = bucket.new_key('logo.png')
	#key.set_contents_from_filename('logo.png')

	#example from string (use for checksum)
	#key = bucket.new_key('hello.txt')
	#key.set_contents_from_string('Hello World!')
	pass

def process_files(bucket_name, source_dir, destination_dir, ncores, perform_checksum, log):
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
				with open(log, 'a') as logfile:
					for result in pool.starmap(dryrun_upload_to_bucket, zip(repeat(bucket_name), folder_files, destination_keys, repeat(perform_checksum))):
						logfile.write(f'{result}\n')

				# testing - stop after 3 folders
				i+=1
				if i == 3:
					break
	# Upload log file
	dryrun_upload_to_bucket(bucket_name, log, os.path.basename(log), False)

if __name__ == '__main__':
	# Initiate timing
	start = datetime.now()
	# Set the source directory, bucket name, and destination directory
	source_dir = "/rds/project/rds-rPTGgs6He74/data/private/VISTA/VIDEO/"
	log = f"{'-'.join(source_dir.split('/')[-2:])}_files.txt"
	destination_dir = "VIDEO"
	folders = []
	folder_files = []
	ncores = 4 # change to adjust number of CPUs (= number of concurrent connections)
	perform_checksum = True
	
	# Setup bucket
	s3_host = 'echo.stfc.ac.uk'
	keys = bucket_manager.get_keys(os.sep.join([os.environ['HOME'],'lsst_keys.json']))
	access_key = keys['access_key']
	secret_key = keys['secret_key']
	
	conn = bucket_manager.get_conn(access_key, secret_key, s3_host)
	
	bucket_name = 'dm-test'
	
	if bucket_name not in [bucket.name for bucket in conn.get_all_buckets()]:
		# commented out while testing
        	#mybucket = conn.create_bucket(mybucket_name)
        	print(f'Added bucket: {bucket_name}')
	else:
        	print(f'Bucket exists: {bucket_name}')
	
	# Process the files in parallel
	print(f'Starting processing at {datetime.now()}, elapsed time = {datetime.now() - start}')
	process_files(bucket_name, source_dir, destination_dir, ncores, perform_checksum, log)

	# Complete
	total_time = datetime.now() - start
	print(f'Finished at {datetime.now()}, elapsed time = {total_time}')
	with open(log, 'r') as logfile:
		file_count = 0
		total_size = 0
	#	file_count = len(logfile.readlines())
		for line in logfile.readlines():
			file_count += 1
			total_size += int(re.sub("[^0-9]", "", line.split()[3]))

	# note added +1 to total_time.seconds for testing - remove later
	print(f'{file_count} files uploaded in {total_time.seconds} seconds, {file_count / (total_time.seconds + 1)} files/sec')
	print(f'{total_size} bytes uploaded in {total_time.seconds} seconds, {total_size / 1024**2 / (total_time.seconds + 1)} MiB/sec')
