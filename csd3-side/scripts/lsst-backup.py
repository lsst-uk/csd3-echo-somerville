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
import subprocess
import yaml

import warnings
warnings.filterwarnings('ignore')

import bucket_manager.bucket_manager as bm

import hashlib
import os
import argparse

def zip_folders(parent_folder,subfolders_to_collate,folders_files,use_compression):
    """
    Collates the specified folders into a zip file.

    Args:
        folders_to_collate (list): A list of folder paths to be included in the zip file.

    Returns:
        bytes: The compressed zip file as a bytes object.
    """
    import io
    import zipfile

    # print(f'subfolders_to_collate: {subfolders_to_collate}')
    # print(f'folders_files: {folders_files}')

    zip_buffer = io.BytesIO()
    if use_compression:
        # zipfile.ZIP_DEFLATED = standard compression
        compression = zipfile.ZIP_DEFLATED
    else:
        # zipfile.ZIP_STORED = no compression
        compression = zipfile.ZIP_STORED
    with zipfile.ZipFile(zip_buffer, "a", compression, True) as zip_file:
        for i,folder in enumerate(subfolders_to_collate):
            for file in folders_files[i]:
                file_path = os.path.join(folder, file)
                # print(f'Adding {file_path} to zip with archive path {os.path.relpath(file_path, parent_folder)}.')
                zip_file.write(file_path, os.path.relpath(file_path, parent_folder))
                print('size',zip_file.__sizeof__())
    # zip_buffer.seek(0)
    print('len',len(zip_buffer.getvalue()))
    return parent_folder, zip_buffer.getvalue()

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
    s3_client = bm.get_client(access_key, secret_key, s3_host)
    bucket = s3.Bucket(bucket_name)
    link = False
    print(f'object_key {object_key}')

    if os.path.islink(filename):
        link = True
    # Check if the file is a symlink
    # If it is, upload an object containing the target path instead
    if link:
        file_data = os.path.realpath(filename)
    else:
        file_data = open(filename, 'rb')
    
    # print(file_data)
    # print(type(file_data))
    # print('length of bytes data:', len(file_data))
    # file_data.seek(0, os.SEEK_END)
    # file_data_size = file_data.tell()
    # file_data.seek(0)

    file_size = os.path.getsize(filename)
    

    print(f'Uploading {filename} from {folder} to {bucket_name}/{object_key}, {file_size} bytes, checksum = {perform_checksum}, dryrun = {dryrun}', flush=True)
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
                try:
                    if file_size > 5 * 1024 * 1024 * 1024:  # Check if file size is larger than 5GiB
                        """
                        - Use multipart upload for large files
                        """
                        print(f'Uploading {filename} to {bucket_name}/{object_key} in parts.')
                        obj = bucket.Object(object_key)
                        mp_upload = obj.initiate_multipart_upload()
                        chunk_size = 500 * 1024 * 1024  # Set chunk size to 500 MiB
                        chunk_count = int(np.ceil(file_size / chunk_size))
                        parts = []
                        for i in range(chunk_count):
                            start = i * chunk_size
                            end = min(start + chunk_size, file_size)
                            part_number = i + 1
                            with open(filename, 'rb') as f:
                                f.seek(start)
                                chunk_data = f.read(end - start)
                            part = s3_client.upload_part(
                                Body=chunk_data,
                                Bucket=bucket_name,
                                Key=object_key,
                                PartNumber=part_number,
                                UploadId=mp_upload.id
                            )
                            parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                        s3_client.complete_multipart_upload(
                            Bucket=bucket_name,
                            Key=object_key,
                            UploadId=mp_upload.id,
                            MultipartUpload={"Parts": parts}
                        )
                    else:
                        """
                        - Upload the file to the bucket
                        """
                        print(f'Uploading {filename} to {bucket_name}/{object_key}')
                        bucket.put_object(Body=file_data, Key=object_key, ContentMD5=checksum_base64)
                except Exception as e:
                    print(f'Error uploading {filename} to {bucket_name}/{object_key}: {e}')
            else:
                try:
                    bucket.put_object(Body=file_data, Key=object_key)
                except Exception as e:
                    print(f'Error uploading {filename} to {bucket_name}/{object_key}: {e}')
            file_data.close()
    """
        report actions
        CSV formatted
        header: LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM
    """
    if link:
        return_string = f'"{folder}","{filename}",{file_size},"{bucket_name}","{object_key}"'
    else:
        return_string = f'"{folder}","{filename}",{file_size},"{bucket_name}","{object_key}"'
    if perform_checksum and not link:
        return_string += f',{checksum_string}'
    else:
        return_string += ',n/a'
    return return_string

def upload_to_bucket_collated(s3_host, access_key, secret_key, bucket_name, folder, file_data, object_key, perform_checksum, dryrun):
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
        file_data (bytes): The file data to upload.
        object_key (str): The key to assign to the uploaded file in the S3 bucket.
        perform_checksum (bool): Flag indicating whether to perform a checksum on the file.
        dryrun (bool): Flag indicating whether to perform a dry run (no actual upload).

    Returns:
        str: A string containing information about the uploaded file in CSV format.
            The format is: LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,CHECKSUM_SIZE,CHECKSUM_KEY
            Where: CHECKSUM, CHECKSUM_SIZE are n/a if checksum was not performed.
    """
    s3 = bm.get_resource(access_key, secret_key, s3_host)
    s3_client = bm.get_client(access_key, secret_key, s3_host)
    bucket = s3.Bucket(bucket_name)
    link = False
    print(f'object_key {object_key}')
    filename = object_key.split('/')[-1]
    file_data_size = len(file_data)

    # print(file_data)
    print(type(file_data))

    # print('length of bytes data:', len(file_data))
    # file_data.seek(0, os.SEEK_END)
    # file_data_size = file_data.tell()
    # file_data.seek(0)
    

    print(f'Uploading zip file "{filename}" for {folder} to {bucket_name}/{object_key}, {file_data_size} bytes, checksum = {perform_checksum}, dryrun = {dryrun}', flush=True)
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
                checksum_hash = hashlib.md5(file_data)
                checksum_string = checksum_hash.hexdigest()
                checksum_base64 = base64.b64encode(checksum_hash.digest()).decode()
                try:
                    if len(file_data) > 5 * 1024 * 1024 * 1024:  # Check if file size is larger than 5GiB
                        """
                        - Use multipart upload for large files
                        """
                        print(f'Uploading "{filename}" ({file_data_size} bytes) to {bucket_name}/{object_key} in parts.')
                        obj = bucket.Object(object_key)
                        mp_upload = obj.initiate_multipart_upload()
                        chunk_size = 500 * 1024 * 1024  # Set chunk size to 500 MiB
                        chunk_count = int(np.ceil(file_data_size / chunk_size))
                        parts = []
                        for i in range(chunk_count):
                            start = i * chunk_size
                            end = min(start + chunk_size, file_data_size)
                            part_number = i + 1
                            with open(filename, 'rb') as f:
                                f.seek(start)
                                chunk_data = f.read(end - start)
                            part = s3_client.upload_part(
                                Body=chunk_data,
                                Bucket=bucket_name,
                                Key=object_key,
                                PartNumber=part_number,
                                UploadId=mp_upload.id
                            )
                            parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                        s3_client.complete_multipart_upload(
                            Bucket=bucket_name,
                            Key=object_key,
                            UploadId=mp_upload.id,
                            MultipartUpload={"Parts": parts}
                        )
                    else:
                        """
                        - Upload the file to the bucket
                        """
                        print(f'Uploading zip file "{filename}" ({file_data_size} bytes) to {bucket_name}/{object_key}')
                        bucket.put_object(Body=file_data, Key=object_key, ContentMD5=checksum_base64)
                except Exception as e:
                    print(f'Error uploading "{filename}" ({file_data_size}) to {bucket_name}/{object_key}: {e}')
            else:
                try:
                    bucket.put_object(Body=file_data, Key=object_key)
                except Exception as e:
                    print(f'Error uploading {filename} to {bucket_name}/{object_key}: {e}')
    """
        report actions
        CSV formatted
        header: LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM
    """
    if link:
        return_string = f'"{folder}","{filename}",{file_data_size},"{bucket_name}","{object_key}"'
    else:
        return_string = f'"{folder}","{filename}",{file_data_size},"{bucket_name}","{object_key}"'
    if perform_checksum and not link:
        return_string += f',{checksum_string}'
    else:
        return_string += ',n/a'
    return return_string


def print_stats(file_name_or_data, file_count, total_size, file_start, file_end, processing_start, total_size_uploaded, total_files_uploaded, collated):
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

    # This give false information as it is called once per file, not once per folder.

    elapsed = file_end - file_start
    if collated:
        print(f'Uploaded zip file, elapsed time = {elapsed}')
    else:
        print(f'Uploaded {file_name_or_data}, elapsed time = {elapsed}')
    elapsed_seconds = elapsed.seconds + elapsed.microseconds / 1e6
    avg_file_size = total_size / file_count / 1024**2
    print(f'{file_count} files (avg {avg_file_size:.2f} MiB/file) uploaded in {elapsed_seconds:.2f} seconds, {elapsed_seconds/file_count:.2f} s/file', flush=True)
    print(f'{total_size / 1024**2:.2f} MiB uploaded in {elapsed_seconds:.2f} seconds, {total_size / 1024**2 / elapsed_seconds:.2f} MiB/s', flush=True)
    print(f'Total elapsed time = {file_end-processing_start}', flush=True)
    print(f'Total files uploaded = {total_files_uploaded}', flush=True)
    print(f'Total size uploaded = {total_size_uploaded / 1024**3:.2f} GiB', flush=True)
    print(f'Running average speed = {total_size_uploaded / 1024**2 / (file_end-processing_start).seconds:.2f} MiB/s', flush=True)
    print(f'Running average rate = {(file_end-processing_start).seconds / total_files_uploaded:.2f} s/file', flush=True)

def upload_and_callback(s3_host, access_key, secret_key, bucket_name, folder, file_name_or_data, object_key, perform_checksum, dryrun, processing_start, file_count, folder_files_size, total_size_uploaded, total_files_uploaded, collated):
    #repeat(s3_host), repeat(access_key), repeat(secret_key), repeat(bucket_name), repeat(folder), folder_files, object_names, repeat(perform_checksum), repeat(dryrun)
    # upload files in parallel and log output
    file_start = datetime.now()
    print(collated)
    if collated:
        try:
            print(f'Uploading zip containing {file_count} subfolders from {folder}.')
            result = upload_to_bucket_collated(s3_host, access_key, secret_key, bucket_name, folder, file_name_or_data, object_key, perform_checksum, dryrun)
        except Exception as e:
            print(f'Error uploading {folder} to {bucket_name}/{object_key}: {e}')
            sys.exit(1)
    else:
        print(f'Uploading {file_count} files from {folder}.')
        result = upload_to_bucket(s3_host, access_key, secret_key, bucket_name, folder, file_name_or_data, object_key, perform_checksum, dryrun)
    
    file_end = datetime.now()
    print_stats(file_name_or_data, file_count, folder_files_size, file_start, file_end, processing_start, total_size_uploaded, total_files_uploaded, collated)
    with open(log, 'a') as logfile:
        logfile.write(f'{result}\n')
    return None

def process_files(s3_host, access_key, secret_key, bucket_name, current_objects, exclude, source_dir, destination_dir, nprocs, perform_checksum, dryrun, log, global_collate, use_compression):
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
    results = []
    to_collate = {} # store folders to collate
    for folder, _, files in os.walk(source_dir):
        # check if folder is in the exclude list
        if folder in exclude:
            print(f'Skipping subfolder {folder} - excluded.')
            continue

        folder_files = [os.sep.join([folder, filename]) for filename in files]
        total_filesize = sum([os.stat(filename).st_size for filename in folder_files])

        # check folder isn't empty
        print(f'Processing {len(files)} files (total size: {total_filesize/1024**2:.0f} MiB) in {folder}.')
        if len(files) > 4 and total_filesize > 96*1024**2 or not global_collate:
            # all files within folder
            print(f'Processing {len(files)} files (total size: {total_filesize}) individually in {folder}.')
            
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
            
            # print('checking for symlinks')
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
            # print(f'{file_count - pre_linkcheck_file_count} symlinks replaced with files. Symlinks renamed to <filename>.symlink')

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
                    repeat(total_files_uploaded),
                    repeat(False),
                )):
                results.append(pool.apply_async(upload_and_callback, args=args))

            if i % nprocs*4 == 0: # have at most 4 times the number of processes in the pool - may be more efficient with higher numbers
                for result in results:
                    result.get()  # Wait until current processes in pool are finished
            
            # release block of files if the list for results is greater than 4 times the number of processes

        elif len(files) > 0 and global_collate:
            parent_folder = os.path.abspath(os.path.join(folder, os.pardir))
            if parent_folder not in to_collate.keys():
                to_collate[parent_folder] = {'parent_folder':parent_folder,'folders':[],'object_names':[], 'folder_files':[], 'zip_data':None, 'zip_object_name':''} # store folders to collate
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
            
            # print('checking for symlinks')
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
            
            # print(f'{file_count - pre_linkcheck_file_count} symlinks replaced with files. Symlinks renamed to <filename>.symlink')
            # print(f'folder {folder} has {len(files)} files (total size: {folder_files_size/1024**2:.0f} MiB); will be uploaded as part of a collated upload.')
            
            to_collate[parent_folder]['folders'].append(folder)
            to_collate[parent_folder]['object_names'].append(object_names)
            to_collate[parent_folder]['folder_files'].append(folder_files)
        else:
            print(f'Skipping subfolder - empty.')
    
    # collate folders
    zip_results = []
    if len(to_collate) > 0:

        print(f'Collating {len([to_collate[parent_folder]["folders"] for parent_folder in to_collate.keys()])} folders into {len(to_collate)} zip files.')
        # call zip_folder in parallel
        # print(to_collate)
        for zip_tuple in to_collate.items():
            parent_folder = zip_tuple[0]
            folders = zip_tuple[1]['folders']
            folder_files = zip_tuple[1]['folder_files']
            print(f'parent_folder before zip: {parent_folder}')
            zip_results.append(pool.apply_async(zip_folders, args=(parent_folder,folders,folder_files,use_compression)))
        for result in zip_results:
            parent_folder, zip_data = result.get()
            print(f'temp_zip_object_name: {parent_folder}/collated.zip')
            to_collate[parent_folder]['zip_data'] = zip_data
            #[os.sep.join([destination_dir, os.path.relpath(filename, source_dir)]) for filename in folder_files]
            to_collate[parent_folder]['zip_object_name'] = str(os.sep.join([destination_dir, os.path.relpath(f'{parent_folder}/collated.zip', source_dir)]))
            print(f"final zip_object_name: {to_collate[parent_folder]['zip_object_name']}")
        
        zip_files_size = np.sum(np.array([len(zip_tuple[1]['zip_data']) for zip_tuple in to_collate.items()]))
        total_size_uploaded += zip_files_size
        total_files_uploaded += len(to_collate)


        print('Uploading collated folders.')
        # print(to_collate.items())
        for zip_tuple in to_collate.items():
            parent_folder = zip_tuple[0]
            zip_data = zip_tuple[1]['zip_data']
            zip_object_name = zip_tuple[1]['zip_object_name']
            print(zip_tuple[1]['parent_folder'])
            print(f"parent_folder here: {parent_folder}")
            print(len(zip_data))
            print(f"zip object name here: {zip_object_name}")
            results.append(pool.apply_async(upload_and_callback, args=(
                s3_host,
                access_key,
                secret_key,
                bucket_name,
                parent_folder,
                zip_data,
                zip_object_name,
                perform_checksum,
                dryrun,
                processing_start,
                1,
                len(zip_data),
                total_size_uploaded,
                total_files_uploaded,
                True,
            )))
            
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
    parser.add_argument('bucket_name', type=str, help='Name of the S3 bucket.')
    parser.add_argument('source_path', type=str, help='Absolute path to the folder to be uploaded.')
    parser.add_argument('S3_prefix', type=str, help='Prefix to be used in S3 object keys.')
    parser.add_argument('S3_folder', type=str, help='Section at the end of the source path to be used in S3 object keys.')
    parser.add_argument('--exclude', nargs='+', help='Folders to exclude from upload as a list or wildcard.')
    parser.add_argument('--nprocs', type=int, default=4, help='Number of CPU cores to use for parallel upload.')
    parser.add_argument('--no-collate', default=False, action='store_true', help='Turn off collation of subfolders containing small numbers of small files into zip files.')
    parser.add_argument('--dryrun', default=False, action='store_true', help='Perform a dry run without uploading files.')
    parser.add_argument('--no-checksum', default=False, action='store_true', help='Do not perform checksum validation during upload.')
    parser.add_argument('--no-compression', default=False, action='store_true', help='Do not use compression when collating files.')
    args = parser.parse_args()
    print(args)
    source_dir = args.source_path
    prefix = args.S3_prefix
    sub_dirs = args.S3_folder
    bucket_name = args.bucket_name
    nprocs = args.nprocs # change to adjust number of CPUs (= number of concurrent connections)
    global_collate = not args.no_collate # internally, flag turns *on* collate, but for user no-collate turns it off - makes flag more intuitive
    perform_checksum = not args.no_checksum
    dryrun = args.dryrun
    use_compression = not args.no_compression

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
    print(f'symlinks will be replaced with the target file. A new file <simlink_file>.symlink will contain the symlink target path.')

    if not source_dir or not prefix or not sub_dirs or not bucket_name:
        parser.print_help()
        sys.exit(1)

    #print hostname
    uname = subprocess.run(['uname', '-n'], capture_output=True)
    print(f'Running on {uname.stdout.decode().strip()}')

    # Initiate timing
    start = datetime.now()
    log_suffix = 'lsst-backup.csv' # DO NOT CHANGE
    log = f"{prefix}-{'-'.join(sub_dirs.split('/'))}-{log_suffix}"
    destination_dir = f"{prefix}/{sub_dirs}" 
    # folders = []
    # folder_files = []
    
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

    # check source_dir formatting
    while source_dir[-1] == '/':
        source_dir = source_dir[:-1]
    
    # Process the files
    print(f'Starting processing at {datetime.now()}, elapsed time = {datetime.now() - start}')
    print(f'Using {nprocs} processes.')
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        process_files(s3_host,access_key, secret_key, bucket_name, current_objects, exclude, source_dir, destination_dir, nprocs, perform_checksum, dryrun, log, global_collate, use_compression)
    
    # Complete
    final_time = datetime.now() - start
    final_time_seconds = final_time.seconds + final_time.microseconds / 1e6
    log_df = pd.read_csv(log)
    log_df = log_df.drop_duplicates(subset='DESTINATION_KEY', keep='last')
    log_df = log_df.reset_index(drop=True)
    log_df.to_csv(log, index=False)

    # Upload log file
    if not dryrun:
        print('Uploading log file.')
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
    
    final_size = log_df["FILE_SIZE"].sum() / 1024**2
    try:
        final_transfer_speed = final_size / final_time_seconds
        
    except ZeroDivisionError:
        final_transfer_speed = 0
    try:
        final_transfer_speed_sperf = final_time_seconds / len(log_df)
    except ZeroDivisionError:
        final_transfer_speed_sperf = 0
    print(f'Finished at {datetime.now()}, elapsed time = {final_time}')
    print(f'Total: {len(log_df)} files; {(final_size):.2f} MiB; {(final_transfer_speed):.2f} MiB/s including setup time; {final_transfer_speed_sperf:.2f} s/file including setup time')
