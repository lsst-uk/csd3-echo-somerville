import os
from multiprocessing import Pool
from itertools import repeat


def dryrun_copy_to_bucket(bucket_name,filename,destination_key,log):
	# Upload the file to the bucket
        #s3.Object(bucket_name, destination_key).upload_file(filename)
	with open(log, 'a') as logfile:
		logfile.write(f'--dryrun-- local: {filename} remote ({bucket_name}): {destination_key}\n')
	

def process_files(bucket_name,source_dir, destination_dir, log):
	i = 0
	with Pool(4) as pool:
		for folder,subfolders,files in os.walk(source_dir):
			print(os.path.relpath(folder, source_dir))
			if len(files) > 0:
				folder_files = [ os.sep.join([folder,filename]) for filename in files ]
				
				destination_keys = [ os.sep.join([destination_dir, os.path.relpath(filename, source_dir)]) for filename in folder_files ]
				#print(destination_keys)
				#print(folder_files[0],destination_keys[0])
				
				pool.starmap(dryrun_copy_to_bucket, zip(repeat(bucket_name), folder_files, destination_keys, repeat(log)))
				i+=1
				if i == 3:
					break

# Set the source directory, bucket name, and destination directory
source_dir = "/rds/project/rds-rPTGgs6He74/data/private/VISTA/VIDEO/"
log = f"{'-'.join(source_dir.split('/')[-2:])}_files.txt"
destination_dir = "VIDEO"
folders = []
folder_files = []
bucket_name = 'lsst-echo-bucket'

if __name__ == '__main__':
	# Process the files in parallel
	process_files(bucket_name, source_dir, destination_dir, log)
