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
    ncores (int): The number of CPU cores to use for parallel processing.
    perform_checksum (bool): Flag indicating whether to perform checksum validation during upload.
    upload_checksum (bool): Flag indicating whether to upload checksum files along with the files.
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
import pandas as pd
import numpy as np


import warnings
warnings.filterwarnings('ignore')
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import bucket_manager.bucket_manager as bm



import hashlib
import os
import argparse

def upload_to_bucket(s3_host, access_key, secret_key, bucket_name, folder, filename, object_key, perform_checksum, upload_checksum, dryrun):
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
        upload_checksum (bool): Flag indicating whether to upload the checksum to the S3 bucket.
        dryrun (bool): Flag indicating whether to perform a dry run (no actual upload).

    Returns:
        str: A string containing information about the uploaded file in CSV format.
            The format is: LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,CHECKSUM_SIZE,CHECKSUM_KEY
            Where: CHECKSUM, CHECKSUM_SIZE and CHECKSUM_KEY are n/a if checksum was not performed and/or a .checksum file was not uploaded.
    """
    s3 = bm.get_resource(access_key, secret_key, s3_host)
    bucket = s3.Bucket(bucket_name)
    file_data = open(filename, 'rb')
    if perform_checksum:
        """
        - Create checksum object
        """
        checksum = hashlib.md5(file_data.read()).hexdigest().encode('utf-8')
        if upload_checksum and not dryrun:
            checksum_key = object_key + '.checksum'
            #create checksum object
            bucket.put_object(Body=checksum, ContentEncoding='utf-8', Key=checksum_key)
    """
    - Upload the file to the bucket
    """
    if not dryrun:
        bucket.upload_fileobj(file_data, object_key)

    """
        report actions
        CSV formatted
        header: LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,CHECKSUM_SIZE,CHECKSUM_KEY
    """
    return_string = f'"{folder}","{filename}",{os.stat(filename).st_size},"{bucket_name}","{object_key}"'
    if perform_checksum and upload_checksum:
        return_string += f',{checksum},{len(checksum)},"{checksum_key}"'
    elif perform_checksum:
        return_string += f',{checksum},n/a,n/a'
    else:
        return_string += ',n/a,n/a,n/a'
    return return_string


def print_stats(folder, file_count, total_size, folder_start, folder_end, upload_checksum):
    """
    Prints the statistics of the upload process.

    Args:
        folder (str): The name of the folder being uploaded.
        file_count (int): The number of files uploaded.
        total_size (int): The total size of the uploaded files in bytes.
        folder_start (datetime): The start time of the folder upload.
        folder_end (datetime): The end time of the folder upload.
        upload_checksum (bool): Indicates whether checksum files were uploaded.

    Returns:
        None
    """
    elapsed = folder_end - folder_start
    print(f'Finished folder {folder}, elapsed time = {elapsed}')
    elapsed_seconds = elapsed.seconds + elapsed.microseconds / 1e6
    avg_file_size = total_size / file_count / 1024**2
    if not upload_checksum:
        print(f'{file_count} files (avg {avg_file_size:.2f} MiB/file) uploaded in {elapsed_seconds:.2f} seconds, {elapsed_seconds/file_count:.2f} s/file', flush=True)
        print(f'{total_size / 1024**2:.2f} MiB uploaded in {elapsed_seconds:.2f} seconds, {total_size / 1024**2 / elapsed_seconds:.2f} MiB/s', flush=True)
    if upload_checksum:
        checksum_size = 32 * file_count  # checksum byte strings are 32 bytes
        total_size += checksum_size
        file_count *= 2
        print(f'{file_count} files (avg {avg_file_size:.2f} MiB/file) uploaded (including checksum files) in {elapsed_seconds:.2f} seconds, {elapsed_seconds/file_count:.2f} s/file', flush=True)
        print(f'{total_size / 1024**2:.2f} MiB uploaded (including checksum files) in {elapsed_seconds:.2f} seconds, {total_size / 1024**2 / elapsed_seconds:.2f} MiB/s', flush=True)

def process_files(s3_host, access_key, secret_key, bucket_name, current_objects, source_dir, destination_dir, ncores, perform_checksum, upload_checksum, dryrun, log):
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
        ncores (int): The number of CPU cores to use for parallel processing.
        perform_checksum (bool): Flag indicating whether to perform checksum validation during upload.
        upload_checksum (bool): Flag indicating whether to upload checksum files along with the files.
        dryrun (bool): Flag indicating whether to perform a dry run without actually uploading the files.
        log (str): The path to the log file.

    Returns:
        None
    """
    i = 0
    #processed_files = []
    with Pool(ncores) as pool: # use 4 CPUs by default - very little speed-up, might drop multiprocessing and parallelise at shell level
        #recursive loop over local folder
        for folder, subfolders, files in os.walk(source_dir):
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
                folder_start = datetime.now()
                
                print('checking for symlinks')
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
                print(f'{file_count - pre_linkcheck_file_count} symlinks replaced with files. Symlinks renamed to <filename>.symlink')
                        
                # upload files in parallel and log output
                print(f'Uploading {file_count} files from {folder} using {ncores} processes.')
                with open(log, 'a') as logfile:
                    for result in pool.starmap(upload_to_bucket, zip(repeat(s3_host), repeat(access_key), repeat(secret_key), repeat(bucket_name), repeat(folder), folder_files, object_names, repeat(perform_checksum), repeat(upload_checksum), repeat(dryrun))):
                        logfile.write(f'{result}\n')
                folder_end = datetime.now()
                folder_files_size = np.sum(np.array([os.path.getsize(filename) for filename in folder_files]))
                print_stats(folder, file_count, folder_files_size, folder_start, folder_end, upload_checksum)

                # testing - stop after 1 folders
                # i+=1
                # if i == 100:
                #     break
            else:
                print(f'Skipping subfolder - empty.')
    # Upload log file
    if not dryrun:
        upload_to_bucket(s3_host, access_key, secret_key, bucket_name, '/', log, os.path.basename(log), False, False, False)


# # Go!
if __name__ == '__main__':
    epilog = '''
where:
    "bucket_name" is the name of the S3 bucket.
    "source_path" is an absolute path to a folder to be uploaded;
    "S3_prefix" is the prefix to be used in S3 object keys;
    "S3_folder" is the section at the end of that path to be used in S3 object keys;
    

example:
    python upload.py my-bucket /home/dave/work/data test data
    would upload files (and non-empty subfolders) from /home/dave/work/data to test/data in my-bucket.
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

    args = parser.parse_args()

    source_dir = args.source_path
    prefix = args.S3_prefix
    sub_dirs = args.S3_folder
    bucket_name = args.bucket_name

    if not source_dir or not prefix or not sub_dirs or not bucket_name:
        parser.print_help()
        sys.exit(1)

    # Initiate timing
    start = datetime.now()

    log = f"{prefix}-{'-'.join(sub_dirs.split('/'))}-files.csv"
    destination_dir = f"{prefix}/{sub_dirs}" 
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
    keys = bm.get_keys()
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
            continue_ = input("Continue? [y/n]\n").lower()
            if continue_ == 'n':
                sys.exit('Bucket exists.')
            elif continue_ == 'y':
                print('Continuing')
            else:
                sys.exit('Invalid input.')
        else:
            print(f'Bucket exists: {bucket_name}')
            print('dryrun == True, so continuing.')
    
    bucket = s3.Bucket(bucket_name)
    current_objects = bm.object_list(bucket)
    
    # Process the files
    print(f'Starting processing at {datetime.now()}, elapsed time = {datetime.now() - start}')
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        process_files(s3_host,access_key,secret_key, bucket_name, current_objects, source_dir, destination_dir, ncores, perform_checksum, upload_checksum, dryrun, log)
    
    # Complete
    final_time = datetime.now() - start
    final_time_seconds = final_time.seconds + final_time.microseconds / 1e6
    log_df = pd.read_csv(log)
    final_size = log_df["FILE_SIZE"].sum() / 1024**2
    print(f'Finished at {datetime.now()}, elapsed time = {final_time}')
    print(f'Total: {len(log_df)} files; {(final_size):.2f} MiB; {(final_size/final_time_seconds):.2f} MiB/s including setup time; {(final_time_seconds/len(log_df)):.2f} s/file including setup time')
