#!/usr/bin/env python
# coding: utf-8
#D.McKay Feb 2024

"""
This script is used to upload files from a local directory to an S3 bucket in parallel.

It it optimised for LSST Butler repositories, where the data is stored in a directory structure with a large number of small files, often with one small file per folder.
In such cases, multiple single-small-file folders are zipped into a single zip file and uploaded to the S3 bucket.
It is expected a secondary system adjacent to the S3 bucket will be used to unzip the files and restore the directory structure to S3.

Usage:
    For usage, see `python lsst-backup.py --help`.

Returns:
    None
"""
import sys
import os
from multiprocessing import Pool
from itertools import repeat
import warnings
from datetime import datetime, timedelta
from time import sleep
import hashlib
import base64
import pandas as pd
import numpy as np
import subprocess
import yaml
import io
import zipfile
import warnings
warnings.filterwarnings('ignore')

import bucket_manager.bucket_manager as bm

import hashlib
import os
import argparse

def zip_folders(parent_folder, subfolders_to_collate, folders_files, use_compression, dryrun, id=0):
    """
    Collates the specified folders into a zip file.

    Args:
        parent_folder (str): The path of the parent folder.
        subfolders_to_collate (list): A list of subfolder paths to be included in the zip file.
        folders_files (list): A list of lists containing files to be included in the zip file for each subfolder.
        use_compression (bool): Flag indicating whether to use compression for the zip file.
        dryrun (bool): Flag indicating whether to perform a dry run without actually creating the zip file.
        id (int, optional): An optional identifier for the zip file. Defaults to 0.

    Returns:
        tuple: A tuple containing the parent folder path, the identifier, and the compressed zip file as a bytes object.

    Raises:
        None

    Examples:
        # Example usage
        parent_folder = "/path/to/parent/folder"
        subfolders_to_collate = ["/path/to/subfolder1", "/path/to/subfolder2"]
        folders_files = [["file1.txt", "file2.txt"], ["file3.txt", "file4.txt"]]
        use_compression = True
        dryrun = False
        id = 1

        result = zip_folders(parent_folder, subfolders_to_collate, folders_files, use_compression, dryrun, id)
        print(result)
        # Output: ("/path/to/parent/folder", 1, b'compressed_zip_file_data')

    """
    if not dryrun:


        zip_buffer = io.BytesIO()
        if use_compression:
            compression = zipfile.ZIP_DEFLATED  # zipfile.ZIP_DEFLATED = standard compression
        else:
            compression = zipfile.ZIP_STORED  # zipfile.ZIP_STORED = no compression
        with zipfile.ZipFile(zip_buffer, "a", compression, True) as zip_file:
            for i, folder in enumerate(subfolders_to_collate):
                for file in folders_files[i]:
                    file_path = os.path.join(folder, file)
                    zip_file.write(file_path, os.path.relpath(file_path, parent_folder))
        return parent_folder, id, zip_buffer.getvalue()
    else:
        return parent_folder, id, b''
    
# def zip_folders_chunked(chunk):
#     """
#     Collates the specified folders into a zip file.

#     Args:
#         chunk: zipped tuple containing:
#             parent_folder (str): The path of the parent folder.
#             subfolders_to_collate (list): A list of subfolder paths to be included in the zip file.
#             folders_files (list): A list of lists containing files to be included in the zip file for each subfolder.
#             use_compression (bool): Flag indicating whether to use compression for the zip file.
#             dryrun (bool): Flag indicating whether to perform a dry run without actually creating the zip file.

#     Returns:
#         tuple: A tuple containing the parent folder path and the compressed zip file as a bytes object.
#     """
#     parent_folder, subfolders_to_collate, folders_files, use_compression, dryrun = chunk
#     if not dryrun:
#         import io
#         import zipfile

#         zip_buffer = io.BytesIO()
#         if use_compression:
#             compression = zipfile.ZIP_DEFLATED  # zipfile.ZIP_DEFLATED = standard compression
#         else:
#             compression = zipfile.ZIP_STORED  # zipfile.ZIP_STORED = no compression
#         with zipfile.ZipFile(zip_buffer, "a", compression, True) as zip_file:
#             for i, folder in enumerate(subfolders_to_collate):
#                 for file in folders_files[i]:
#                     file_path = os.path.join(folder, file)
#                     zip_file.write(file_path, os.path.relpath(file_path, parent_folder))
#         return parent_folder, zip_buffer.getvalue()
#     else:
#         return parent_folder, b''

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
    else:
        checksum_string = "DRYRUN"
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
    #for no zip contents
    return_string += ',n/a'
    return return_string

def upload_to_bucket_collated(s3_host, access_key, secret_key, bucket_name, folder, file_data, zip_contents, object_key, perform_checksum, dryrun):
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
        file_data (bytes): The file data to upload (zipped).
        zip_contents (list): A list of files included in the zip file (file_data).
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
    # print(f'object_key {object_key}')
    filename = object_key.split('/')[-1]
    file_data_size = len(file_data)

    # print(file_data)
    # print(type(file_data))

    # print('length of bytes data:', len(file_data))
    # file_data.seek(0, os.SEEK_END)
    # file_data_size = file_data.tell()
    # file_data.seek(0)
    

    print(f'Uploading zip file "{filename}" for {folder} to {bucket_name}/{object_key}, {file_data_size} bytes, checksum = {perform_checksum}, dryrun = {dryrun}', flush=True)
    """
    - Upload the file to the bucket
    """
    if not dryrun:
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
                    bucket.put_object(Body=file_data, Key=object_key, ContentMD5=checksum_base64, Metadata={'zip-contents': ','.join(zip_contents)})
            except Exception as e:
                print(f'Error uploading "{filename}" ({file_data_size}) to {bucket_name}/{object_key}: {e}')
        else:
            try:
                bucket.put_object(Body=file_data, Key=object_key, Metadata={'zip_contents': ','.join(zip_contents)})
            except Exception as e:
                print(f'Error uploading {filename} to {bucket_name}/{object_key}: {e}')
   
    else:
        checksum_string = "DRYRUN"
    """
        report actions
        CSV formatted
        header: LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,ZIP_CONTENTS
    """
    return_string = f'"{folder}","{filename}",{file_data_size},"{bucket_name}","{object_key}"'
    if perform_checksum:
        return_string += f',{checksum_string}'
    else:
        return_string += ',n/a'
    return_string += f',"{",".join(zip_contents)}"'
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

def upload_and_callback(s3_host, access_key, secret_key, bucket_name, folder, file_name_or_data, zip_contents, object_key, perform_checksum, dryrun, processing_start, file_count, folder_files_size, total_size_uploaded, total_files_uploaded, collated):
    #repeat(s3_host), repeat(access_key), repeat(secret_key), repeat(bucket_name), repeat(folder), folder_files, object_names, repeat(perform_checksum), repeat(dryrun)
    # upload files in parallel and log output
    file_start = datetime.now()
    print(f'collated = {collated}')
    if collated:
        try:
            print(f'Uploading zip containing {file_count} subfolders from {folder}.')
            result = upload_to_bucket_collated(s3_host, access_key, secret_key, bucket_name, folder, file_name_or_data, zip_contents, object_key, perform_checksum, dryrun)
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

# def free_up_zip_memory(to_collate, parent_folder, index):
#     del to_collate[parent_folder]['zips'][index]['zip_contents']
#     print(f'Deleted zip contents object for {parent_folder}, zip index {index}, to free memory.')

def process_files(s3_host, access_key, secret_key, bucket_name, current_objects, exclude, local_dir, destination_dir, nprocs, perform_checksum, dryrun, log, global_collate, use_compression):
    """
    Uploads files from a local directory to an S3 bucket in parallel.

    Args:
        s3_host (str): The hostname of the S3 server.
        access_key (str): The access key for the S3 server.
        secret_key (str): The secret key for the S3 server.
        bucket_name (str): The name of the S3 bucket.
        current_objects (list): A list of object names already present in the S3 bucket.
        local_dir (str): The local directory containing the files to upload.
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
    if global_collate:
        half_cores = nprocs // 2
        zip_pool = Pool(processes=half_cores)
        collate_ul_pool = Pool(processes=nprocs - half_cores)
    
    pool = Pool(nprocs) # use 4 CPUs by default - very little speed-up, might drop multiprocessing and parallelise at shell level
    #recursive loop over local folder
    results = []
    to_collate = {} # store folders to collate
    
    for folder, sub_folders, files in os.walk(local_dir, topdown=True):
        # print(f'Processing {folder}.')
        # print(f'Files: {files}')
        # print(f'Subfolders: {sub_folders}')
        # continue
        # check if folder is in the exclude list
        if len(files) == 0 and len(sub_folders) == 0:
            print(f'Skipping subfolder - no files or subfolders.')
            continue
        elif len(files) == 0:
            print(f'Skipping subfolder - no files.')
            continue
        if exclude.isin([folder]).any():
            print(f'Skipping subfolder {folder} - excluded.')
            continue
        # remove subfolders in exclude list
        if len(sub_folders) > 0:
            len_pre_exclude = len(sub_folders)
            sub_folders[:] = [sub_folder for sub_folder in sub_folders if not exclude.isin([sub_folder]).any()]
            # sub_folders[:] = [sub_folder for sub_folder in sub_folders if sub_folder not in exclude]
            print(f'Skipping {len_pre_exclude - len(sub_folders)} subfolders in {folder} - excluded. {len(sub_folders)} subfolders remaining.')

        folder_files = [os.sep.join([folder, filename]) for filename in files]
        total_filesize = sum([os.stat(filename).st_size for filename in folder_files])
        if total_filesize > 0:
            mean_filesize = total_filesize / len(files)
        else:
            mean_filesize = 0

        # check if any subfolders contain no subfolders and < 4 files
        if len(sub_folders) > 0:
            for sub_folder in sub_folders:
                sub_folder_path = os.path.join(folder, sub_folder)
                _, sub_sub_folders, sub_files = next(os.walk(sub_folder_path), ([], [], []))
                subfolder_files = [os.sep.join([sub_folder_path, filename]) for filename in sub_files]
                total_subfilesize = sum([os.stat(filename).st_size for filename in subfolder_files])
                if not sub_sub_folders and len(sub_files) < 4 and total_subfilesize < 96*1024**2:
                    sub_folders.remove(sub_folder) # not sure what the effect of this is
                    # upload files in subfolder "as is" i.e., no zipping

        # check folder isn't empty
        print(f'Processing {len(files)} files (total size: {total_filesize/1024**2:.0f} MiB) in {folder} with {len(sub_folders)} subfolders.')
        # len(files) > 2 taken out to give increased number of zip files
        if mean_filesize > 128*1024**2 or not global_collate:
            # all files within folder
            # print(f'Processing {len(files)} files (total size: {total_filesize}) individually in {folder}.')
            
            # keys to files on s3
            object_names = [os.sep.join([destination_dir, os.path.relpath(filename, local_dir)]) for filename in folder_files]
            # print(f'folder_files: {folder_files}')
            # print(f'object_names: {object_names}')
            init_len = len(object_names)
            # remove current objects - avoids reuploading
            # could provide overwrite flag if this is desirable
            # print(f'current_objects: {current_objects}')
            if current_objects.isin(object_names).all():
                #all files in this subfolder already in bucket
                print(f'Skipping subfolder - all files exist.')
                continue
            for oni, on in enumerate(object_names):
                if current_objects.isin([on]).any():
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
            print(f'{file_count - pre_linkcheck_file_count} symlinks replaced with files. Symlinks renamed to <filename>.symlink')

            print(f'Sending {file_count} files (total size: {folder_files_size/1024**2:.0f} MiB) in {folder} to S3 bucket {bucket_name}.')

            for i,args in enumerate(
                zip(
                    repeat(s3_host), 
                    repeat(access_key), 
                    repeat(secret_key), 
                    repeat(bucket_name), 
                    repeat(folder), 
                    folder_files,
                    repeat(None),
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

            if i > nprocs*4 and i % nprocs*4 == 0: # have at most 4 times the number of processes in the pool - may be more efficient with higher numbers
                for result in results:
                    result.get()  # Wait until current processes in pool are finished
            
            # release block of files if the list for results is greater than 4 times the number of processes

        elif len(files) > 0 and global_collate: # small files in folder
            folder_files_size = np.sum(np.array([os.lstat(filename).st_size for filename in folder_files]))
            parent_folder = os.path.abspath(os.path.join(folder, os.pardir))
            if parent_folder not in to_collate.keys():
                #initialise parent folder
                to_collate[parent_folder] = {'parent_folder':parent_folder,'folders':[],'object_names':[], 'folder_files':[], 'zips':[{'zip_data':None, 'id':None, 'zip_object_name':''}]} # store folders to collate
            # keys to files on s3
            object_names = [os.sep.join([destination_dir, os.path.relpath(filename, local_dir)]) for filename in folder_files]
            # print(f'folder_files: {folder_files}')
            # print(f'object_names: {object_names}')
            init_len = len(object_names)
            # remove current objects - avoids reuploading
            # could provide overwrite flag if this is desirable
            # print(f'current_objects: {current_objects}')
            if current_objects.isin(object_names).all():
                #all files in this subfolder already in bucket
                print(f'Skipping subfolder - all files exist.')
                continue
            for oni, on in enumerate(object_names):
                if current_objects.isin([on]).any():
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
    
    # collate folders
    zip_results = []
    if len(to_collate) > 0:
        # print(f"zips: {to_collate[parent_folder]['zips']}")
        print(f'Collating {len([to_collate[parent_folder]["folders"] for parent_folder in to_collate.keys()])} folders into zip files.') #{sum([len(x["zips"]) for x in to_collate.keys()])}
        # call zip_folder in parallel
        # print(to_collate)
        for zip_tuple in to_collate.items():
            parent_folder = zip_tuple[0]
            folders = zip_tuple[1]['folders']
            folder_files = zip_tuple[1]['folder_files']
            chunks = [folders[i:i + nprocs] for i in range(0, len(folders), nprocs)]
            chunk_files = [folder_files[i:i + nprocs] for i in range(0, len(folder_files), nprocs)]
            if len(chunks) != len(chunk_files):
                print('Error: chunks and chunk_files are not the same length.')
                sys.exit(1)
            print(f'parent_folder above zip(s): {parent_folder}')
            print(f'collating into: {len(chunks)} zip file(s)')
            for id,chunk in enumerate(zip(chunks,chunk_files)):
                # print(f'chunk {id} contains {len(chunk[0])} folders')
                zip_results.append(zip_pool.apply_async(zip_folders, (parent_folder,chunk[0],chunk_files[0],use_compression,dryrun,id)))
        zipped = 0
        uploaded = []
        total_zips = len(zip_results)
        while zipped < total_zips:
            for i, result in enumerate(zip_results):
                if result is not None:
                    if result.ready():
                        parent_folder, id, zip_data = result.get()
                        zip_results[i] = None # remove from list to free memory
                        if (parent_folder,id) in uploaded:
                            continue
                        else:
                            zipped += 1
                            uploaded.append((parent_folder,id))
                            print(f'Zipped {zipped} of {len(zip_results)} zip files.', flush=True)
                            # print(parent_folder, id, uploaded, flush=True)
                            with zipfile.ZipFile(io.BytesIO(zip_data), 'r') as z:
                                zip_contents = z.namelist()
                            to_collate[parent_folder]['zips'].append({'zip_contents':zip_contents, 'id':id, 'zip_object_name':str(os.sep.join([destination_dir, os.path.relpath(f'{parent_folder}/collated_{id}.zip', local_dir)]))})
                            # #[os.sep.join([destination_dir, os.path.relpath(filename, local_dir)]) for filename in folder_files]
                            # to_collate[parent_folder][id]['zip_object_name'] = 

                            # tc_index = len(to_collate[parent_folder]['zips']) - 1

                            # check if zip_object_name exists in bucket and get its checksum
                            if current_objects.isin([to_collate[parent_folder]['zips'][-1]['zip_object_name']]).any():
                                existing_zip_checksum = bm.get_resource(access_key, secret_key, s3_host).Object(bucket_name,to_collate[parent_folder]['zips'][-1]['zip_object_name']).e_tag.strip('"')
                                checksum_hash = hashlib.md5(zip_data)
                                checksum_string = checksum_hash.hexdigest()

                                if checksum_string == existing_zip_checksum:
                                    print(f'Zip file {to_collate[parent_folder]["zips"][-1]["zip_object_name"]} already exists and checksums match - skipping.')
                                    continue
                                
                            # upload zipped folders
                            total_size_uploaded += len(zip_data)
                            total_files_uploaded += 1
                            print(f"Uploading {to_collate[parent_folder]['zips'][-1]['zip_object_name']}.")
                            results.append(collate_ul_pool.apply_async(upload_and_callback, args=(
                                s3_host,
                                access_key,
                                secret_key,
                                bucket_name,
                                parent_folder,
                                zip_data,
                                to_collate[parent_folder]['zips'][-1]['zip_contents'],
                                to_collate[parent_folder]['zips'][-1]['zip_object_name'],
                                perform_checksum,
                                dryrun,
                                processing_start,
                                1,
                                len(zip_data),
                                total_size_uploaded,
                                total_files_uploaded,
                                True),
                                # callback=lambda _: free_up_zip_memory(to_collate, parent_folder, tc_index),
                            ))

    pool.close()
    pool.join()
    if global_collate:
        zip_pool.close()
        zip_pool.join()
        collate_ul_pool.close()
        collate_ul_pool.join()

# # Go!
if __name__ == '__main__':
    epilog = ''
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
    parser.add_argument('--config-file', type=str, help='Path to the configuration YAML file.')
    parser.add_argument('--bucket-name', type=str, help='Name of the S3 bucket.')
    parser.add_argument('--local-path', type=str, help='Absolute path to the folder to be uploaded.')
    parser.add_argument('--S3-prefix', type=str, help='Prefix to be used in S3 object keys.')
    parser.add_argument('--S3-folder', type=str, help='Subfolder(s) at the end of the local path to be used in S3 object keys.', nargs='?', const='', default='')
    parser.add_argument('--exclude', nargs='+', help="Folders to exclude from upload as a list in the form ['dir1', 'dir2', ...] or other valid YAML. Must be subdorectories of the specified local path, given as relative paths.")
    parser.add_argument('--nprocs', type=int, help='Number of CPU cores to use for parallel upload.')
    parser.add_argument('--no-collate', default=False, action='store_true', help='Turn off collation of subfolders containing small numbers of small files into zip files.')
    parser.add_argument('--dryrun', default=False, action='store_true', help='Perform a dry run without uploading files.')
    parser.add_argument('--no-checksum', default=False, action='store_true', help='Do not perform checksum validation during upload.')
    parser.add_argument('--no-compression', default=False, action='store_true', help='Do not use compression when collating files.')
    parser.add_argument('--save-config', default=False, action='store_true', help='Save the configuration to the provided config file path and exit.')
    args = parser.parse_args()

    if not args.config_file and not (args.bucket_name and args.local_path and args.S3_prefix):
        parser.error('If a config file is not provided, the bucket name, local path, and S3 prefix must be provided.')
    if args.config_file and (args.bucket_name or args.local_path or args.S3_prefix or args.S3_folder or args.exclude or args.nprocs or args.no_collate or args.dryrun or args.no_checksum or args.no_compression):
        print(f'WARNING: Options provide on command line override options in {args.config_file}.')
    if args.config_file:
        config_file = args.config_file
        if not os.path.exists(config_file) and not args.save_config:
            sys.exit(f'Config file {config_file} does not exist.')
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
                if 'bucket_name' in config.keys() and not args.bucket_name:
                    args.bucket_name = config['bucket_name']
                if 'local_path' in config.keys() and not args.local_path:
                    args.local_path = config['local_path']
                if 'S3_prefix' in config.keys() and not args.S3_prefix:
                    args.S3_prefix = config['S3_prefix']
                if 'S3_folder' in config.keys() and not args.S3_folder:
                    args.S3_folder = config['S3_folder']
                if 'exclude' in config.keys() and not args.exclude:
                    args.exclude = config['exclude']
                if 'nprocs' in config.keys() and not args.nprocs:
                    args.nprocs = config['nprocs']
                if 'nprocs' not in config.keys() and not args.nprocs: # required to allow default value of 4 as this overrides "default" in add_argument
                    args.nprocs = 4
                if 'no_collate' in config.keys() and not args.no_collate:
                    args.no_collate = config['no_collate']
                if 'dryrun' in config.keys() and not args.dryrun:
                    args.dryrun = config['dryrun']
                if 'no_checksum' in config.keys() and not args.no_checksum:
                    args.no_checksum = config['no_checksum']
                if 'no_compression' in config.keys() and not args.no_compression:
                    args.no_compression = config['no_compression']
    if args.save_config and not args.config_file:
        parser.error('A config file must be provided to save the configuration.')

    save_config = args.save_config
    bucket_name = args.bucket_name
    local_dir = args.local_path
    if not os.path.exists(local_dir):
        sys.exit(f'Local path {local_dir} does not exist.')
    prefix = args.S3_prefix
    sub_dirs = args.S3_folder
    print(f'sub_dirs {sub_dirs}')
    nprocs = args.nprocs 
    global_collate = not args.no_collate # internally, flag turns *on* collate, but for user no-collate turns it off - makes flag more intuitive
    perform_checksum = not args.no_checksum # internally, flag turns *on* checksumming, but for user no-checksum  turns it off - makes flag more intuitive
    dryrun = args.dryrun
    use_compression = not args.no_compression # internally, flag turns *on* compression, but for user no-compression turns it off - makes flag more intuitive

    
    if args.exclude:
        exclude = pd.Series(args.exclude)
    else:
        exclude = pd.Series([])
    
    print(f'Config: {args}')

    if save_config:
        with open(config_file, 'w') as f:
            yaml.dump({'bucket_name': bucket_name, 'local_path': local_dir, 'S3_prefix': prefix, 'S3_folder': sub_dirs, 'exclude': exclude.to_list(), 'nprocs': nprocs, 'no_collate': not global_collate, 'dryrun': dryrun, 'no_checksum': not perform_checksum, 'no_compression': not use_compression}, f)
        sys.exit(0)

    print(f'Symlinks will be replaced with the target file. A new file <simlink_file>.symlink will contain the symlink target path.')

    if not local_dir or not prefix or not bucket_name:
        parser.print_help()
        sys.exit(1)
    
    #print hostname
    uname = subprocess.run(['uname', '-n'], capture_output=True)
    print(f'Running on {uname.stdout.decode().strip()}')

    # Initiate timing
    start = datetime.now()
    
    ##allow top-level folder to be provided with S3-folder == ''
    if sub_dirs == '':
        log_suffix = 'lsst-backup.csv' # DO NOT CHANGE
        log = f"{prefix}-{log_suffix}"
        # check for previous suffix (remove after testing)
        previous_suffix = 'files.csv'
        previous_log = f"{prefix}-{previous_suffix}"
        destination_dir = f"{prefix}"
    else:
        log_suffix = 'lsst-backup.csv' # DO NOT CHANGE
        log = f"{prefix}-{'-'.join(sub_dirs.split('/'))}-{log_suffix}"
        # check for previous suffix (remove after testing)
        previous_suffix = 'files.csv'
        previous_log = f"{prefix}-{'-'.join(sub_dirs.split('/'))}-{previous_suffix}"
        destination_dir = f"{prefix}/{sub_dirs}" 

    # folders = []
    # folder_files = []
    
    # Add titles to log file
    if not os.path.exists(log):
        if os.path.exists(previous_log):
            # rename previous log
            os.rename(previous_log, log)
            print(f'Renamed {previous_log} to {log}')
        else:
            # create new log
            print(f'Created backup log file {log}')
            with open(log, 'a') as logfile: # don't open as 'w' in case this is a continuation
                logfile.write('LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,ZIP_CONTENTS\n')
    
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
        else:
            print(f'Bucket exists: {bucket_name}')
            print('dryrun == True, so continuing.')
    
    bucket = s3.Bucket(bucket_name)
    print(f'Getting current object list for {bucket_name}. This may take some time.\nStarting at {datetime.now()}, elapsed time = {datetime.now() - start}')
    current_objects = bm.object_list(bucket)
    print(f'Done.\nFinished at {datetime.now()}, elapsed time = {datetime.now() - start}')

    current_objects = pd.Series(current_objects)
    ## check if log exists in the bucket, and download it and append top it if it does
    # TODO: integrate this with local check for log file
    if current_objects.isin([log]).any():
        print(f'Log file {log} already exists in bucket. Downloading.')
        bucket.download_file(log, log)
    elif current_objects.isin([previous_log]).any():
        print(f'Previous log file {previous_log} already exists in bucket. Downloading.')
        bucket.download_file(previous_log, log)

    # check local_dir formatting
    while local_dir[-1] == '/':
        local_dir = local_dir[:-1]
    
    
    # Process the files
    print(f'Starting processing at {datetime.now()}, elapsed time = {datetime.now() - start}')
    print(f'Using {nprocs} processes.')
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        process_files(s3_host,access_key, secret_key, bucket_name, current_objects, exclude, local_dir, destination_dir, nprocs, perform_checksum, dryrun, log, global_collate, use_compression)
    
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
