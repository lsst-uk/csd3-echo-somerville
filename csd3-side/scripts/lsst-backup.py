#!/usr/bin/env python
# coding: utf-8
#D.McKay Feb 2024



"""
This script is used to upload files from a local directory to an S3 bucket in parallel.

Usage:
    python upload.py source_path prefix sub_dirs

Where:
    - "source_path" is an absolute path to a folder to be uploaded.
    - "sub_dirs" is the section at the end of that path to be used in S3 object keys.
    - "prefix" is the prefix to be used in S3 object keys.

Example:
    python upload.py /home/dave/work/data test /data
    Would upload files (and non-empty subfolders) from /home/dave/work/data to test/data.

The script utilizes multiprocessing to upload files in parallel. It calculates checksums for the files and can optionally upload checksum files along with the files. It also provides statistics on the upload process, including the number of files uploaded, total size, and elapsed time.

The main function in this script is "process_files", which takes the following arguments:

Args:
    s3_host (str): The hostname of the S3 server.
    access_key (str): The access key for the S3 server.
    secret_key (str): The secret key for the S3 server.
    bucket_name (str): The name of the S3 bucket.
    current_objects (list): A list of object names already present in the S3 bucket.
    source_dir (str): The local directory containing the files to upload.
    destination_dir (str): The destination directory in the S3 bucket.
    nprocs (int): The number of CPU cores to use for parallel processing.
    perform_checksum (bool): Flag indicating whether to perform checksum validation during upload.
    dryrun (bool): Flag indicating whether to perform a dry run without actually uploading the files.
    log (str): The path to the log file.

Returns:
    None
"""
import sys
import os
from multiprocessing import Pool
from itertools import repeat
import warnings
from datetime import datetime
from time import sleep
import hashlib
import base64
import pandas as pd
import numpy as np
import glob



import warnings
warnings.filterwarnings('ignore')

import bucket_manager.bucket_manager as bm



import hashlib
import os
import argparse

def upload_to_bucket(s3_host, access_key, secret_key, bucket_name, folder, filename, object_key, perform_checksum, dryrun):
    """
    Uploads a file to an S3 bucket.
    Optionally calculates a checksum for the file
    Optionally uploads the checksum to file.ext.checksum alongside file.ext.
    
    Args:
        s3_host (str): The S3 host URL.
        access_key (str): The access key for the S3 bucket.
        secret_key (str): The secret key for the S3 bucket.
        bucket_name (str): The name of the S3 bucket.
        folder (str): The local folder containing the file to upload.
        filename (str): The name of the file to upload.
        object_key (str): The key to assign to the uploaded file in the S3 bucket.
        perform_checksum (bool): Flag indicating whether to perform a checksum on the file.
        dryrun (bool): Flag indicating whether to perform a dry run (no actual upload).

    Returns:
        str: A string containing information about the uploaded file in CSV format.
            The format is: LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,CHECKSUM_SIZE,CHECKSUM_KEY
            Where: CHECKSUM, CHECKSUM_SIZE are n/a if checksum was not performed.
    """
    s3 = bm.get_resource(access_key, secret_key, s3_host)
    bucket = s3.Bucket(bucket_name)
    link = False
    if os.path.islink(filename):
        link = True
    # Check if the file is a symlink
    # If it is, upload an object containing the target path instead
    if link:
        file_data = os.path.realpath(filename)
    else:
        file_data = open(filename, 'rb')
 
    
    """
    - Upload the file to the bucket
    """
    if not dryrun:
        if link:
            """
            - Upload the link target _path_ to an object
            """
            bucket.put_object(Body=file_data, Key=object_key)
        if not link:
            if perform_checksum:
                """
                - Create checksum object
                """
                file_data.seek(0)  # Ensure we're at the start of the file
                checksum_hash = hashlib.md5(file_data.read())
                checksum_string = checksum_hash.hexdigest()
                checksum_base64 = base64.b64encode(checksum_hash.digest()).decode()
                file_data.seek(0)  # Reset the file pointer to the start
                bucket.put_object(Body=file_data, Key=object_key, ContentMD5=checksum_base64)
                file_data.close()
            else:
                bucket.put_object(Body=file_data, Key=object_key)
    """
        report actions
        CSV formatted
        header: LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM
    """
    if link:
        return_string = f'"{folder}","{filename}",{os.lstat(filename).st_size},"{bucket_name}","{object_key}"'
    else:
        return_string = f'"{folder}","{filename}",{os.stat(filename).st_size},"{bucket_name}","{object_key}"'
    if perform_checksum and not link:
        return_string += f',{checksum_string}'
    else:
        return_string += ',n/a'
    return return_string


def print_stats(folder, file_count, total_size, folder_start, folder_end, processing_start, total_size_uploaded, total_files_uploaded):
    """
    Prints the statistics of the upload process.

    Args:
        folder (str): The name of the folder being uploaded.
        file_count (int): The number of files uploaded.
        total_size (int): The total size of the uploaded files in bytes.
        folder_start (datetime): The start time of the folder upload.
        folder_end (datetime): The end time of the folder upload.
        processing_elapsed (time_delta): The total elapsed time for the upload process.

    Returns:
        None
    """
    elapsed = folder_end - folder_start
    print(f'Finished folder {folder}, elapsed time = {elapsed}')
    elapsed_seconds = elapsed.seconds + elapsed.microseconds / 1e6
    avg_file_size = total_size / file_count / 1024**2
    print(f'{file_count} files (avg {avg_file_size:.2f} MiB/file) uploaded in {elapsed_seconds:.2f} seconds, {elapsed_seconds/file_count:.2f} s/file', flush=True)
    print(f'{total_size / 1024**2:.2f} MiB uploaded in {elapsed_seconds:.2f} seconds, {total_size / 1024**2 / elapsed_seconds:.2f} MiB/s', flush=True)
    print(f'Total elapsed time = {folder_end-processing_start}')
    print(f'Total files uploaded = {total_files_uploaded}')
    print(f'Total size uploaded = {total_size_uploaded / 1024**3:.2f} GiB')

def upload_and_callback(s3_host, access_key, secret_key, bucket_name, folder, filename, object_key, perform_checksum, dryrun, processing_start, file_count, folder_files_size, total_size_uploaded, total_files_uploaded):
    #repeat(s3_host), repeat(access_key), repeat(secret_key), repeat(bucket_name), repeat(folder), folder_files, object_names, repeat(perform_checksum), repeat(dryrun)
    folder_start = datetime.now()
    result = upload_to_bucket(s3_host, access_key, secret_key, bucket_name, folder, filename, object_key, perform_checksum, dryrun)
    folder_end = datetime.now()
    print_stats(folder, file_count, folder_files_size, folder_start, folder_end, processing_start, total_size_uploaded, total_files_uploaded)
    with open(log, 'a') as logfile:
        logfile.write(f'{result}\n')
    return None

def process_files(s3_host, access_key, secret_key, bucket_name, current_objects, exclude, source_dir, destination_dir, nprocs, perform_checksum, dryrun, log):
    """
    Uploads files from a local directory to an S3 bucket in parallel.

    Args:
        s3_host (str): The hostname of the S3 server.
        access_key (str): The access key for the S3 server.
        secret_key (str): The secret key for the S3 server.
        bucket_name (str): The name of the S3 bucket.
        current_objects (list): A list of object names already present in the S3 bucket.
        source_dir (str): The local directory containing the files to upload.
        destination_dir (str): The destination directory in the S3 bucket.
        nprocs (int): The number of CPU cores to use for parallel processing.
        perform_checksum (bool): Flag indicating whether to perform checksum validation during upload.
        dryrun (bool): Flag indicating whether to perform a dry run without actually uploading the files.
        log (str): The path to the log file.

    Returns:
        None
    """
    processing_start = datetime.now()
    total_size_uploaded = 0
    total_files_uploaded = 0
    i = 0
    #processed_files = []
    pool = Pool(nprocs) # use 4 CPUs by default - very little speed-up, might drop multiprocessing and parallelise at shell level
    #recursive loop over local folder
    for folder, subfolders, files in os.walk(source_dir):
        # check if folder is in the exclude list
        if folder in exclude:
            print(f'Skipping subfolder {folder} - excluded.')
            continue
        # check folder isn't empty
        if len(files) > 0:
            # all files within folder
            folder_files = [os.sep.join([folder, filename]) for filename in files]
            # keys to files on s3
            object_names = [os.sep.join([destination_dir, os.path.relpath(filename, source_dir)]) for filename in folder_files]
            # print(f'folder_files: {folder_files}')
            # print(f'object_names: {object_names}')
            init_len = len(object_names)
            # remove current objects - avoids reuploading
            # could provide overwrite flag if this is desirable
            # print(f'current_objects: {current_objects}')
            if all([obj in current_objects for obj in object_names]):
                #all files in this subfolder already in bucket
                print(f'Skipping subfolder - all files exist.')
                continue
            for oni, on in enumerate(object_names):
                if on in current_objects:
                    object_names.remove(on)
                    del folder_files[oni]
            pre_linkcheck_file_count = len(object_names)
            if init_len - pre_linkcheck_file_count > 0:
                print(f'Skipping {init_len - pre_linkcheck_file_count} existing files.')
            # print(f'folder_files: {folder_files}')
            # print(f'object_names: {object_names}')
            # folder_start = datetime.now()
            
            print('checking for symlinks')
            #always do this AFTER removing "current_objects" to avoid re-uploading
            symlink_targets = []
            symlink_obj_names = []
            for i in range(len(folder_files)):
                if os.path.islink(folder_files[i]):
                    #rename link in object_names
                    symlink_obj_name = object_names[i]
                    object_names[i] = '.'.join([object_names[i], 'symlink'])
                    #add symlink target to symlink_targets list
                    symlink_targets.append(os.path.realpath(folder_files[i]))
                    #add real file to symlink_obj_names list
                    symlink_obj_names.append(symlink_obj_name)

            # append symlink_targets and symlink_obj_names to folder_files and object_names

            folder_files.extend(symlink_targets)
            object_names.extend(symlink_obj_names)

            file_count = len(object_names)
            # folder_end = datetime.now()
            folder_files_size = np.sum(np.array([os.lstat(filename).st_size for filename in folder_files]))
            total_size_uploaded += folder_files_size
            total_files_uploaded += file_count
            print(f'{file_count - pre_linkcheck_file_count} symlinks replaced with files. Symlinks renamed to <filename>.symlink')

            # upload files in parallel and log output
            print(f'Uploading {file_count} files from {folder} using {nprocs} processes.')
            results=[]
            for i,args in enumerate(
                zip(
                    repeat(s3_host), 
                    repeat(access_key), 
                    repeat(secret_key), 
                    repeat(bucket_name), 
                    repeat(folder), 
                    folder_files, 
                    object_names, 
                    repeat(perform_checksum), 
                    repeat(dryrun), 
                    repeat(processing_start),
                    repeat(file_count),
                    repeat(folder_files_size),
                    repeat(total_size_uploaded),
                    repeat(total_files_uploaded)
                )):
                results.append(pool.apply_async(upload_and_callback, args=args))
                if i % nprocs*4 == 0:
                    for result in results:
                        result.get()  # Wait until current processes in pool are finished
                    results = []
        else:
            print(f'Skipping subfolder - empty.')
    # Upload log file
    if not dryrun:
        upload_to_bucket(s3_host,
                         access_key, 
                         secret_key, 
                         bucket_name, 
                         '/', #path
                         log, 
                         os.path.basename(log), 
                         False, # perform_checksum
                         False, # dryrun
                        )
    pool.close()
    pool.join()


# # Go!
if __name__ == '__main__':
    epilog = '''
where:
    "bucket_name" is the name of the S3 bucket.
    "source_path" is an absolute path to a folder to be uploaded;
    "S3_prefix" is the prefix to be used in S3 object keys;
    "S3_folder" is the section at the end of that path to be used in S3 object keys;
    

example:
    python upload.py my-bucket /home/dave/work/data test data --exclude do_not_backup*
    would upload files (and non-empty subfolders) from /home/dave/work/data to test/data in my-bucket,
    excluding files and folders starting in /home/dave/work/data that start with "do_not_backup".
'''
    class MyParser(argparse.ArgumentParser):
        def error(self, message):
            sys.stderr.write(f'error: {message}\n\n')
            self.print_help()
            sys.exit(2)
    parser = MyParser(
        description='Upload files from a local directory to an S3 bucket in parallel.',
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('bucket_name', type=str, help='Name of the S3 bucket')
    parser.add_argument('source_path', type=str, help='Absolute path to the folder to be uploaded')
    parser.add_argument('S3_prefix', type=str, help='Prefix to be used in S3 object keys')
    parser.add_argument('S3_folder', type=str, help='Section at the end of the source path to be used in S3 object keys')
    parser.add_argument('--exclude', nargs='+', help='Folders to exclude from upload as a list or wildcard')
    parser.add_argument('--nprocs', type=int, default=4, help='Number of CPU cores to use for parallel upload')
    args = parser.parse_args()

    source_dir = args.source_path
    prefix = args.S3_prefix
    sub_dirs = args.S3_folder
    bucket_name = args.bucket_name
    nprocs = args.nprocs # change to adjust number of CPUs (= number of concurrent connections)
    exclude = []
    if args.exclude:
        if '*' not in ''.join(args.exclude):
            # treat as list
            exclude = [f'{source_dir}/{excl}' for excl in args.exclude[0].split(',')]
        else:
            # treat as wildcard string
            exclude = [item for sublist in [glob.glob(f'{source_dir}/{excl}') for excl in args.exclude] for item in sublist]
            # exclude = glob.glob(f'{source_dir}/{args.exclude}')
    print(f'Excluding {exclude}')

    if not source_dir or not prefix or not sub_dirs or not bucket_name:
        parser.print_help()
        sys.exit(1)

    # Initiate timing
    start = datetime.now()
    log_suffix = 'lsst-backup.csv' # DO NOT CHANGE
    log = f"{prefix}-{'-'.join(sub_dirs.split('/'))}-{log_suffix}"
    destination_dir = f"{prefix}/{sub_dirs}" 
    # folders = []
    # folder_files = []
    
    perform_checksum = True
    dryrun = False
    
    # Add titles to log file
    if not os.path.exists(log):
        # check for previous suffix (remove after testing)
        previous_suffix = 'files.csv'
        previous_log = f"{prefix}-{'-'.join(sub_dirs.split('/'))}-{previous_suffix}"
        if os.path.exists(previous_log):
            # rename previous log
            os.rename(previous_log, log)
            print(f'Renamed {previous_log} to {log}')
        else:
            # create new log
            print(f'Created backup log file {log}')
            with open(log, 'a') as logfile: # don't open as 'w' in case this is a continuation
                logfile.write('LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM\n')
    
    # Setup bucket
    s3_host = 'echo.stfc.ac.uk'
    try:
        keys = bm.get_keys()
    except KeyError as e:
        print(e)
        sys.exit()
    access_key = keys['access_key']
    secret_key = keys['secret_key']
    
    s3 = bm.get_resource(access_key, secret_key, s3_host)
    bucket_list = bm.bucket_list(s3)
    
    if bucket_name not in bucket_list:
        if not dryrun:
                s3.create_bucket(Bucket=bucket_name)
                print(f'Added bucket: {bucket_name}')
    else:
        if not dryrun:
            print(f'Bucket exists: {bucket_name}')
            print('Existing files will be skipped.')
            # continue_ = input("Continue? [y/n]\n").lower()
            # if continue_ == 'n':
            #     sys.exit('Bucket exists.')
            # elif continue_ == 'y':
            #     print('Continuing')
            # else:
            #     sys.exit('Invalid input.')
        else:
            print(f'Bucket exists: {bucket_name}')
            print('dryrun == True, so continuing.')
    
    bucket = s3.Bucket(bucket_name)
    current_objects = bm.object_list(bucket)
    
    # Process the files
    print(f'Starting processing at {datetime.now()}, elapsed time = {datetime.now() - start}')
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        process_files(s3_host,access_key, secret_key, bucket_name, current_objects, exclude, source_dir, destination_dir, nprocs, perform_checksum, dryrun, log)
    
    # Complete
    final_time = datetime.now() - start
    final_time_seconds = final_time.seconds + final_time.microseconds / 1e6
    log_df = pd.read_csv(log)
    final_size = log_df["FILE_SIZE"].sum() / 1024**2
    print(f'Finished at {datetime.now()}, elapsed time = {final_time}')
    print(f'Total: {len(log_df)} files; {(final_size):.2f} MiB; {(final_size/final_time_seconds):.2f} MiB/s including setup time; {(final_time_seconds/len(log_df)):.2f} s/file including setup time')
