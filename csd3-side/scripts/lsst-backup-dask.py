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
import gc
import sys
import os
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
from psutil import virtual_memory
warnings.filterwarnings('ignore')
from logging import ERROR

import bucket_manager.bucket_manager as bm

import hashlib
import os
import argparse

from dask.distributed import Client, get_client, wait, as_completed

from typing import List

def find_metadata(key: str, bucket) -> List[str]:
    """
    Finds the metadata for a given key in an S3 bucket.

    Args:
        key (dd.core.Scalar or str): The key to search for metadata.
        s3: The S3 object.

    Returns:
        list[str]: A list of existing metadata contents if found, otherwise empty list.
    """
    if type(key) == str:
        existing_zip_contents = None
        if key.endswith('.zip'):
            try:
                existing_zip_contents = str(bucket.Object(''.join([key,'.metadata'])).get()['Body'].read().decode('UTF-8')).split('|') # use | as separator
            except Exception as e:
                try:
                    existing_zip_contents = bucket.Object(key).metadata['zip-contents'].split('|') # use | as separator
                except KeyError:
                    return None
            if existing_zip_contents:
                return existing_zip_contents
        else:
            return None
    else:
        return None


def remove_duplicates(l: list[dict]) -> list[dict]:
    return pd.DataFrame(l).drop_duplicates().to_dict(orient='records')

# def zip_folders(parent_folder:str, subfolders_to_collate:list[str], folders_files:list[str], use_compression:bool, dryrun:bool, processing_start, id:int, mem_per_worker:int, total_size_uploaded: int, total_files_uploaded:int) -> tuple[str, int, bytes]:
def zip_folders(parent_folder:str, subfolders_to_collate:list[str], folders_files:list[str], use_compression:bool, dryrun:bool, id:int, mem_per_worker:int) -> tuple[str, int, bytes]:
    """
    Collates the specified folders into a zip file.

    Args:
        parent_folder (str): The path of the parent folder.
        subfolders_to_collate (list): A list of subfolder paths to be included in the zip file.
        folders_files (list): A list of lists containing files to be included in the zip file for each subfolder.
        use_compression (bool): Flag indicating whether to use compression for the zip file.
        dryrun (bool): Flag indicating whether to perform a dry run without actually creating the zip file.
        id (int, optional): An optional identifier for the zip file. Defaults to 0.
        mem_per_worker (int): The memory per worker in bytes.

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
    zipped_size = 0
    # print(f'parent_folder: {parent_folder}')
    # print(f'subfolders_to_collate: {subfolders_to_collate}')
    # print(f'folders_files: {folders_files}')
    # exit()
    client = get_client()
    if not dryrun:
        try:
            zip_buffer = io.BytesIO()
            if use_compression:
                compression = zipfile.ZIP_DEFLATED  # zipfile.ZIP_DEFLATED = standard compression
            else:
                compression = zipfile.ZIP_STORED  # zipfile.ZIP_STORED = no compression
            with zipfile.ZipFile(zip_buffer, "a", compression, True) as zip_file:
                # for i, folder in enumerate(subfolders_to_collate):
                    for file in folders_files:
                        # print(f'file: {file}', flush=True)
                        if file.startswith('/'):
                            file_path = file
                        else:
                            file_path = os.path.join(subfolders_to_collate, file)
                        arc_name = os.path.relpath(file_path, parent_folder)
                        zipped_size += os.path.getsize(file_path)
                        with open(file_path, 'rb') as src_file:
                            zip_file.writestr(arc_name, src_file.read())
                    # zip_contents = zip_file.namelist()
            if zipped_size > mem_per_worker:
                print(f'WARNING: Zipped size of {zipped_size} bytes exceeds memory per core of {mem_per_worker} bytes.')
        except MemoryError as e:
            print(f'Error zipping {parent_folder}: {e}')
            print(f'Namespace: {globals()}')
            exit(1)
        # client.scatter([parent_folder, id, zip_buffer.getvalue()], broadcast=True)
        return parent_folder, id, zip_buffer.getvalue()
    else:
        return parent_folder, id, b''
    
def part_uploader(s3_host, access_key, secret_key, bucket_name, object_key, part_number, chunk_data, upload_id) -> dict:
    """
    Uploads a part of a file to an S3 bucket.

    Args:
        s3_host (str): The host URL of the S3 service.
        access_key (str): The access key for the S3 service.
        secret_key (str): The secret key for the S3 service.
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The key of the object in the S3 bucket.
        part_number (int): The part number of the chunk being uploaded.
        chunk_data (bytes): The data of the chunk being uploaded.
        upload_id (str): The ID of the ongoing multipart upload.

    Returns:
        dict: A dictionary containing the part number and ETag of the uploaded part.
    """
    s3_client = bm.get_client(access_key, secret_key, s3_host)
    return {"PartNumber":part_number,
            "ETag":s3_client.upload_part(Body=chunk_data,
                          Bucket=bucket_name,
                          Key=object_key,
                          PartNumber=part_number,
                          UploadId=upload_id)["ETag"]}
    
def upload_to_bucket(s3_host, access_key, secret_key, bucket_name, folder, filename, object_key, perform_checksum, dryrun, mem_per_worker) -> str:
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
        mem_per_worker (int): The memory per worker in bytes.

    Returns:
        str: A string containing information about the uploaded file in CSV format.
            The format is: LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,CHECKSUM_SIZE,CHECKSUM_KEY
            Where: CHECKSUM, CHECKSUM_SIZE are n/a if checksum was not performed.
    """
    s3 = bm.get_resource(access_key, secret_key, s3_host)
    s3_client = bm.get_client(access_key, secret_key, s3_host)
    bucket = s3.Bucket(bucket_name)
    link = False
    # print(f'object_key {object_key}')

    if os.path.islink(filename):
        link = True
    # Check if the file is a symlink
    # If it is, upload an object containing the target path instead
    if link:
        file_data = os.path.realpath(filename)
    else:
        file_data = open(filename, 'rb')

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
                    if file_size > mem_per_worker or file_size > 5 * 1024**3:  # Check if file size is larger than 5GiB
                        """
                        - Use multipart upload for large files
                        """
                        
                        obj = bucket.Object(object_key)
                        mp_upload = obj.initiate_multipart_upload()
                        chunk_size = 512 * 1024**2  # 512 MiB
                        chunk_count = int(np.ceil(file_size / chunk_size))
                        print(f'Uploading {filename} to {bucket_name}/{object_key} in {chunk_count} parts.')
                        parts = []
                        part_futures = []
                        for i in range(chunk_count):
                            start = i * chunk_size
                            end = min(start + chunk_size, file_size)
                            part_number = i + 1
                            with open(filename, 'rb') as f:
                                f.seek(start)
                                chunk_data = f.read(end - start)
                            part_futures.append(get_client().submit(
                                part_uploader, 
                                s3_host,
                                access_key,
                                secret_key,
                                bucket_name,
                                object_key,
                                part_number,
                                chunk_data,
                                mp_upload.id
                            ))
                        for future in as_completed(part_futures):
                            parts.append(future.result())
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

    del file_data # Delete the file data to free up memory
    #gc.collect()

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

def upload_to_bucket_collated(s3_host, access_key, secret_key, bucket_name, folder, file_data, zip_contents, object_key, perform_checksum, dryrun, mem_per_worker) -> str:
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

    filename = object_key.split('/')[-1]
    file_data_size = len(file_data)
    
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
                if file_data_size > 5 * 1024**3:  # Check if file size is larger than 5GiB
                    """
                    - Use multipart upload for large files
                    """
                    # Do metadata first so its URI can be added to up_upload on initiation
                    metadata_value = '\0'.join(zip_contents) # use null byte as separator
                    metadata_object_key = object_key + '.metadata'
                    bucket.put_object(Body=metadata_value, Key=metadata_object_key, Metadata={'corresponding-zip': object_key})
                    metadata = {'zip-contents-object': metadata_object_key}
                    
                    obj = bucket.Object(object_key)
                    mp_upload = obj.initiate_multipart_upload(Metadata=metadata)
                    chunk_size = 512 * 1024**2  # 512 MiB
                    try:
                        chunk_count = int(np.ceil(file_data_size / chunk_size))
                    except ZeroDivisionError:
                        exit(1)
                    print(f'Uploading "{filename}" ({file_data_size} bytes) to {bucket_name}/{object_key} in {chunk_count} parts.', flush=True)

                    parts = []
                    part_futures = []
                    for i in range(chunk_count):
                        start = i * chunk_size
                        end = min(start + chunk_size, file_data_size)
                        part_number = i + 1
                        chunk_data = file_data[start:end]
                        part_futures.append(get_client().submit(
                                part_uploader, 
                                s3_host,
                                access_key,
                                secret_key,
                                bucket_name,
                                object_key,
                                part_number,
                                chunk_data,
                                mp_upload.id
                            ))
                    for future in as_completed(part_futures):
                        parts.append(future.result())
                    s3_client.complete_multipart_upload(
                        Bucket=bucket_name,
                        Key=object_key,
                        UploadId=mp_upload.id,
                        MultipartUpload={"Parts": parts}
                    )
                        # part = s3_client.upload_part(
                        #     Body=chunk_data,
                        #     Bucket=bucket_name,
                        #     Key=object_key,
                        #     PartNumber=part_number,
                        #     UploadId=mp_upload.id
                        # )
                        # parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                    # s3_client.complete_multipart_upload(
                    #     Bucket=bucket_name,
                    #     Key=object_key,
                    #     UploadId=mp_upload.id,
                    #     MultipartUpload={"Parts": parts}
                    # )
                    
                else:
                    """
                    - Upload the file to the bucket
                    """
                    print(f'Uploading zip file "{filename}" ({file_data_size} bytes) to {bucket_name}/{object_key}')
                    metadata_value = '|'.join(zip_contents) # use | as separator
                    metadata_size = len(metadata_value.encode('utf-8'))

                    # print(f'Metadata size: {metadata_size} bytes', flush=True)
                    # print(f'Metadata value: {metadata_value}', flush=True)

                    if metadata_size > 1024:
                        metadata_object_key = object_key + '.metadata'
                        print(f'Metadata size exceeds the size limit. Writing to {metadata_object_key}.', flush=True)
                        bucket.put_object(Body=metadata_value, Key=metadata_object_key, Metadata={'corresponding-zip': object_key})
                        metadata = {'zip-contents-object': metadata_object_key}
                    else:
                        # print('Metadata size is within the limit.', flush=True)
                        metadata = {'zip-contents': metadata_value}

                    bucket.put_object(Body=file_data, Key=object_key, ContentMD5=checksum_base64, Metadata=metadata)
            except Exception as e:
                print(f'Error uploading "{filename}" ({file_data_size}) to {bucket_name}/{object_key}: {e}')
                exit(1)
        else:
            try:
                bucket.put_object(Body=file_data, Key=object_key, Metadata={'zip-contents': '|'.join(zip_contents)}) # use | as separator
            except Exception as e:
                print(f'Error uploading {filename} to {bucket_name}/{object_key}: {e}')
                exit(1)
    else:
        checksum_string = "DRYRUN"

    del file_data # Delete the file data to free up memory
    #gc.collect()

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

def print_stats(file_name_or_data, file_count, total_size, file_start, file_end, processing_start, total_size_uploaded, total_files_uploaded, collated) -> None:
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
    del file_name_or_data
    #gc.collect()

def upload_and_callback(s3_host, access_key, secret_key, bucket_name, folder, file_name_or_data, zip_contents, object_key, perform_checksum, dryrun, processing_start, file_count, folder_files_size, total_size_uploaded, total_files_uploaded, collated, mem_per_worker) -> None:
    # upload files in parallel and log output
    file_start = datetime.now()
    print(f'collated = {collated}', flush=True)
    if collated:
        try:
            print(f'Uploading zip containing {file_count} subfolders from {folder}.')
            result = upload_to_bucket_collated(s3_host, access_key, secret_key, bucket_name, folder, file_name_or_data, zip_contents, object_key, perform_checksum, dryrun, mem_per_worker)
            # print(result)
        except Exception as e:
            print(f'Error uploading {folder} to {bucket_name}/{object_key}: {e}')
            sys.exit(1)
    else:
        print(f'Uploading {file_count} files from {folder}.')
        result = upload_to_bucket(s3_host, access_key, secret_key, bucket_name, folder, file_name_or_data, object_key, perform_checksum, dryrun, mem_per_worker)
    
    file_end = datetime.now()
    print_stats(file_name_or_data, file_count, folder_files_size, file_start, file_end, processing_start, total_size_uploaded, total_files_uploaded, collated)
    with open(log, 'a') as logfile:
        logfile.write(f'{result}\n')
    
    del file_name_or_data
    #gc.collect()

    return None

def process_files(s3_host, access_key, secret_key, bucket_name, current_objects, exclude, local_dir, destination_dir, perform_checksum, dryrun, log, global_collate, use_compression, client, mem_per_worker) -> None:
    """
    Uploads files from a local directory to an S3 bucket in parallel.

    Args:
        s3_host (str): The hostname of the S3 server.
        access_key (str): The access key for the S3 server.
        secret_key (str): The secret key for the S3 server.
        bucket_name (str): The name of the S3 bucket.
        current_objects (ps.Dataframe): A list of object names already present in the S3 bucket.
        local_dir (str): The local directory containing the files to upload.
        destination_dir (str): The destination directory in the S3 bucket.
        perform_checksum (bool): Flag indicating whether to perform checksum validation during upload.
        dryrun (bool): Flag indicating whether to perform a dry run without actually uploading the files.
        log (str): The path to the log file.
        global_collate (bool): Flag indicating whether to collate files into zip files before uploading.
        use_compression (bool): Flag indicating whether to use compression for the zip files.
        client (dask.Client): The Dask client object.
        mem_per_worker (int): The memory per worker in bytes.

    Returns:
        None
    """
    processing_start = datetime.now()
    total_size_uploaded = 0
    total_files_uploaded = 0
    i = 0
    #processed_files = []
    # if global_collate:
    #     half_cores = nprocs // 2
    #     zip_pool = Pool(processes=half_cores)
    #     collate_ul_pool = Pool(processes=nprocs - half_cores)
    
    #pool = Pool(nprocs) # use 4 CPUs by default - very little speed-up, might drop multiprocessing and parallelise at shell level

    client.scatter([s3_host, access_key, secret_key, bucket_name, perform_checksum, dryrun, current_objects], broadcast=True)
        
    #recursive loop over local folder
    # results = []
    to_collate = {} # store folders to collate
    total_all_folders = 0
    total_all_files = 0
    folder_num = 0
    file_num = 0
    uploads = []
    zip_uploads = []
    upload_futures = []
    zip_futures = []
    zul_futures = []
    failed = []
    for folder, sub_folders, files in os.walk(local_dir, topdown=True):
        total_all_folders += 1
        total_all_files += len(files)
    
    for folder, sub_folders, files in os.walk(local_dir, topdown=True):
        #gc.collect()
        folder_num += 1
        file_num += len(files)
        print(f'Processing {folder_num}/{total_all_folders} folders; {file_num}/{total_all_files} files in {local_dir}.')
        
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
            print(f'Skipping {len_pre_exclude - len(sub_folders)} subfolders in {folder} - excluded. {len(sub_folders)} subfolders remaining.')

        folder_files = [os.sep.join([folder, filename]) for filename in files]

        sizes = []
        for f in zul_futures:
            if f.status == 'finished':
                del f
        for f in upload_futures:
            if f.status == 'finished':
                del f
        for filename in folder_files:
            # print(os.path.relpath(filename, local_dir))
            if exclude.isin([os.path.relpath(filename, local_dir)]).any():
                print(f'Skipping file {filename} - excluded.')
                folder_files.remove(filename)
                if len(folder_files) == 0:
                    print(f'Skipping subfolder - no files - see exclusions.')
                continue
            try:
                sizes.append(os.stat(filename).st_size)
            except PermissionError:
                print(f'WARNING: Permission error reading {filename}. File will not be backed up.')
                folder_files.remove(filename)
                if len(folder_files) == 0:
                    print(f'Skipping subfolder - no files - see permissions warning(s).')
                    continue
        total_filesize = sum(sizes)
        if total_filesize > 0:
            mean_filesize = total_filesize / len(files)
        else:
            mean_filesize = 0
        
        # if results is not []:
        #     if virtual_memory().available * 0.5 < total_filesize or mean_filesize > mem_per_core:
        #         for result in results:
        #             result.get()
        #         #gc.collect()

        # check if any subfolders contain no subfolders and < 4 files
        if len(sub_folders) > 0:
            for sub_folder in sub_folders:
                sub_folder_path = os.path.join(folder, sub_folder)
                _, sub_sub_folders, sub_files = next(os.walk(sub_folder_path), ([], [], []))
                subfolder_files = [os.sep.join([sub_folder_path, filename]) for filename in sub_files]
                subfiles_sizes = []
                for filename in subfolder_files:
                    try:
                        subfiles_sizes.append(os.stat(filename).st_size)
                    except PermissionError:
                        print(f'WARNING: Permission error reading {filename}. File will not be backed up.')
                        subfolder_files.remove(filename)
                        if len(subfolder_files) == 0:
                            print(f'Skipping subfolder - no files - see permissions warning(s).')
                            continue
                total_subfilesize = sum(subfiles_sizes)
                if not sub_sub_folders and len(sub_files) < 4 and total_subfilesize < 96*1024**2:
                    sub_folders.remove(sub_folder) # not sure what the effect of this is
                    # upload files in subfolder "as is" i.e., no zipping

        # check folder isn't empty
        print(f'Processing {len(folder_files)} files (total size: {total_filesize/1024**2:.0f} MiB) in {folder} with {len(sub_folders)} subfolders.')
        # len(files) > 2 taken out to give increased number of zip files

        # keys to files on s3
        object_names = [os.sep.join([destination_dir, os.path.relpath(filename, local_dir)]) for filename in folder_files]
        # print(f'folder_files: {folder_files}')
        # print(f'object_names: {object_names}')
        init_len = len(object_names)
        # remove current objects - avoids reuploading
        # could provide overwrite flag if this is desirable
        # print(f'current_objects: {current_objects}')
        if not current_objects.empty and current_objects['CURRENT_OBJECTS'].isin(object_names).all():
            #all files in this subfolder already in bucket
            print(f'Skipping subfolder - all files exist.')
            continue


        if mean_filesize > 256*1024**2 or not global_collate:
            # all files within folder
            # print(f'Processing {len(files)} files (total size: {total_filesize}) individually in {folder}.')
            
            
            # if uploading file individually, remove existing files from object_names
            if not current_objects.empty:
                for oni, on in enumerate(object_names):
                    if current_objects['CURRENT_OBJECTS'].isin([on]).any() or current_objects['CURRENT_OBJECTS'].isin([f'{on}.symlink']).any():
                        object_names.remove(on)
                        # print(f'Removing {on} in object_names - previously uploaded.', flush=True)
                        del folder_files[oni]
                # else:
                    # print(f'Keeping {on} in object_names - not previously uploaded.', flush=True)
            pre_linkcheck_file_count = len(object_names)
            if init_len - pre_linkcheck_file_count > 0:
                print(f'Skipping {init_len - pre_linkcheck_file_count} existing files.')
            # print(f'folder_files: {folder_files}')
            # print(f'object_names: {object_names}')
            # folder_start = datetime.now()
            # print(object_names, flush=True)
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
            # print(object_names, flush=True)
            # append symlink_targets and symlink_obj_names to folder_files and object_names

            folder_files.extend(symlink_targets)
            object_names.extend(symlink_obj_names)

            file_count = len(object_names)
            # folder_end = datetime.now()
            folder_files_size = np.sum(np.array([os.stat(filename).st_size for filename in folder_files]))
            total_size_uploaded += folder_files_size
            total_files_uploaded += file_count
            print(f'{file_count - pre_linkcheck_file_count} symlinks replaced with files. Symlinks renamed to <filename>.symlink')

            print(f'Sending {file_count} files (total size: {folder_files_size/1024**2:.0f} MiB) in {folder} to S3 bucket {bucket_name}.')
            
            try:
                for i,args in enumerate(zip(
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
                        repeat(mem_per_worker),
                    )):
                    upload_futures.append(client.submit(upload_and_callback, *args))
                    # if folder_files_size > 5*1024**3:
                    #     print(f'WARNING: Total size of {folder_files_size} bytes exceeds 5 GiB. Waiting and purging.')
                    #     wait(upload_futures[-1])
                    #     del upload_futures[-1]
                        #gc.collect()
                    uploads.append({'folder':args[4],'folder_size':args[12],'file_size':os.lstat(folder_files[i]).st_size,'file':args[5],'object':args[7],'uploaded':False})
            except BrokenPipeError as e:
                print(f'Caught BrokenPipeError: {e}')
                # Record the failure
                with open('error_log.err', 'a') as f:
                    f.write(f'BrokenPipeError: {e}\n')
                # Exit gracefully
                sys.exit(1)
            except Exception as e:
                print(f'An unexpected error occurred: {e}')
                # Record the failure
                with open('error_log.err', 'a') as f:
                    f.write(f'Unexpected error: {e}\n')
                # Exit gracefully
                sys.exit(1)    
                # if i > nprocs*4 and i % nprocs*4 == 0: # have at most 4 times the number of processes in the pool - may be more efficient with higher numbers
                #     for result in results:
                #         result.get()  # Wait until current processes in pool are finished
            
            # release block of files if the list for results is greater than 4 times the number of processes
            # #gc.collect()

        elif len(folder_files) > 0 and global_collate: # small files in folder
            folder_files_size = np.sum(np.array([os.lstat(filename).st_size for filename in folder_files]))
            parent_folder = os.path.abspath(os.path.join(folder, os.pardir))
            print(f'parent_folder: {parent_folder}')
            # possibly pass if parent_folder == local_dir or parent_folder contains '..'
            if parent_folder not in to_collate.keys():
                #initialise parent folder
                to_collate[parent_folder] = {'parent_folder':parent_folder,'folders':[],'object_names':[], 'folder_files':[], 'zips':[{'zip_data':None, 'id':None, 'zip_object_name':''}]} # store folders to collate
            

            #Don't remove existing files from object_names?
            # for oni, on in enumerate(object_names):
            #     if current_objects.isin([on]).any() or current_objects.isin([f'{on}.symlink']).any():
            #         object_names.remove(on)
            #         print(f'Removing {on} from object_names - previously uploaded.')
            #         del folder_files[oni]
            #     else:
            #         print(f'Keeping {on} in object_names - not previously uploaded.')

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
            
            ###############################
            # CHECK HERE FOR ZIP CONTENTS #
            ###############################
            these_zip_contents = [ff.replace(parent_folder+'/','') for ff in folder_files]
            if not current_objects.empty:
                if current_objects['METADATA'].isin([these_zip_contents]).any():
                    existing_zip_contents = current_objects[current_objects['METADATA'].isin([these_zip_contents])]['METADATA'].values[0]
                    if all([x in existing_zip_contents for x in these_zip_contents]):
                        print(f'Zip file {to_collate[parent_folder]["zips"][-1]["zip_object_name"]} already exists and file lists match - skipping.')
                        # zip_results[i] = None
                        del to_collate[parent_folder]
                        continue
                    else:
                        print(f'Zip file {to_collate[parent_folder]["zips"][-1]["zip_object_name"]} already exists but file lists do not match - reuploading.')

            to_collate[parent_folder]['folders'].append(folder)
            to_collate[parent_folder]['object_names'].append(object_names)
            to_collate[parent_folder]['folder_files'].append(folder_files)

        
        # if len(zul_futures) > 1000:
        #     print(f'WARNING: >1000 zipfiles in memory. Purging.')
        
                    #gc.collect()
        print(f'Pending file uploads: {len([None for f in upload_futures if f.status == "pending"])}')
        print(f'Pending zips: {len([None for f in zip_futures if f.status == "pending"])}')
        print(f'Pending zip uploads: {len([None for f in zul_futures if f.status == "pending"])}')
    
    # collate folders
    # total_zips = 0
    if len(to_collate) > 0:
        # print(f"zips: {to_collate[parent_folder]['zips']}")
        print(f'Collating {len([to_collate[parent_folder]["folders"] for parent_folder in to_collate.keys()])} folders into zip files.') #{sum([len(x["zips"]) for x in to_collate.keys()])}
        # print(f'parent_folders: {to_collate.keys()}')
        # call zip_folder in parallel
        # print(to_collate)
        for zip_tuple in to_collate.items():
            parent_folder = zip_tuple[0]
            if os.path.abspath(parent_folder) == os.path.abspath(os.sep.join([local_dir,'..'])):
                continue
            folders = zip_tuple[1]['folders']
            folder_files = zip_tuple[1]['folder_files']
            num_files = sum([len(ff) for ff in folder_files])
            # print(f'num_files = {num_files}')
            # print(f'folders: {len(folders)}')
            # print(f'folder_files: {len(folder_files)}')
            try:
                max_filesize = max([max([os.lstat(filename).st_size for filename in ff]) for ff in folder_files])
                # folder_size = sum([sum([os.lstat(filename).st_size for filename in ff]) for ff in folder_files])
            except ValueError:
                # no files in folder - likely removed from file list due to previous PermissionError - continue without message
                continue
            
            max_files_per_zip = int(np.ceil(1024**3 / max_filesize)) # limit zips to 1 GiB - using available memory too inconsistent
            num_zips = int(np.ceil(num_files / max_files_per_zip))
            # print(f'num_zips = {num_zips}')
            
            chunk_subfolders = False
            if num_zips > len(folders):
                chunk_subfolders = True

            if chunk_subfolders:
                # print('in: chunking subfolders')
                # print('folders:',folders)
                # print('folder_files:',folder_files)
                # print('num_zips:',num_zips)
                subchunks_files = []
                subchunks = []
                for j in range(len(folders)):
                    for i in range(0, len(folder_files[j]), len(folder_files[j])//num_zips):
                        # print(f'folder_files[{j}][{i}]: {folder_files[j][i]}')
                        subchunks_files.append(folder_files[j][i:i+len(folder_files[j])//num_zips])
                        subchunks.append(folders[j])
                # print('subchunks_files:',subchunks_files)
                # print('subchunks:',subchunks)
                chunks = subchunks
                chunk_files = subchunks_files
            else:
                chunks = folders
                chunk_files = folder_files

            if len(chunks) != len(chunk_files):
                print('Error: chunks and chunk_files are not the same length.')
                print(len(chunks), chunks)
                print(len(chunk_files), chunk_files)
                sys.exit(1)
            # print(f'parent_folder above zip(s): {parent_folder}')
            # print(f'collating into: {len(chunks)} zip file(s)')
            # total_zips += len(chunks)
            # print(f'parent_folder: {parent_folder}')
            # print(f'chunks: {chunks}')
            # print(f'chunk_files: {chunk_files}')
            # print(f'len(chunks): {len(chunks)}')
            # print(f'len(chunk_files): {len(chunk_files)}')
            # print(f'use_compression: {use_compression}')
            # print(f'dryrun: {dryrun}')
            # print(f'mem_per_core: {mem_per_core}')
            # print(f'folder_size: {folder_size}')
            # for id,chunk in enumerate(zip(chunks,chunk_files)):
            #     # print(f'chunk {id} contains {len(chunk[0])} folders')
         
            for i, args in enumerate(zip(
                    repeat(parent_folder),
                    chunks,
                    chunk_files,
                    repeat(use_compression),
                    repeat(dryrun),
                    # repeat(processing_start),
                    [i for i in range(len(chunks))],
                    repeat(mem_per_worker),
                    # repeat(total_size_uploaded),
                    # repeat(total_files_uploaded),
                    )):
                zip_futures.append(client.submit(
                    zip_folders,
                    *args
                ))
                # if i > nprocs*4/2 and i % nprocs*4/2 == 0: # have at most 4 times the number of processes in the pool - may be more efficient with higher numbers
                #     for result in results:
                #         result.get()  # Wait until current processes in pool are finished

            for zip_future in as_completed(zip_futures):
                parent_folder, id, zip_data = zip_future.result()
                
                with zipfile.ZipFile(io.BytesIO(zip_data), 'r') as z:
                    zip_contents = z.namelist()
                # print(f'zip contents: {zip_contents}')
                to_collate[parent_folder]['zips'].append({'zip_contents':zip_contents, 'id':id, 'zip_object_name':str(os.sep.join([destination_dir, os.path.relpath(f'{parent_folder}/collated_{id}.zip', local_dir)]))})

                # upload zipped folders
                total_size_uploaded += len(zip_data)
                total_files_uploaded += 1
                print(f"Uploading {to_collate[parent_folder]['zips'][-1]['zip_object_name']}.")
                zul_futures.append(client.submit(upload_and_callback, 
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
                        True,
                        mem_per_worker,
                ))
                zip_uploads.append({'folder':parent_folder,'size':len(zip_data),'object_name':to_collate[parent_folder]['zips'][-1]['zip_object_name'],'uploaded':False}) # removed ,'zip_contents':to_collate[parent_folder]['zips'][-1]['zip_contents']
                if len(zip_data) > 5*1024**3:
                    wait(zul_futures[-1])
                    del zul_futures[-1]
                del zip_data
                del zip_future
                

                
        
        # This code isn't accessed early enough - find better monitoring method
        
        monitor_interval = datetime.now()
        while True:
            if datetime.now() - monitor_interval > timedelta(seconds=10):
                # these prints make no sense as futures are deleted after completion
                print(f'Zipped {sum([f.done() for f in zip_futures])} of {len(zip_futures)} zip files.', flush=True)
                print(f'Uploaded {sum([f.done() for f in zul_futures])} of {len(zul_futures)} zip files.', flush=True)
                print(f'Uploaded {sum([f.done() for f in upload_futures])} of {len(upload_futures)} files.', flush=True)
                print(f'Failed uploads: {len(failed)} of {len(zul_futures)+len(upload_futures)}', flush=True)
                monitor_interval = datetime.now()

            for f in upload_futures+zul_futures:
                if 'exception' in f.status and f not in failed:
                    f_tuple = f.exception(), f.traceback()
                    if f_tuple not in failed:
                        failed.append(f_tuple)
            for finished in [f for f in upload_futures+zul_futures if f.status == 'finished']:
                del finished
                #gc.collect()
            # for zip_future in [zf for zf in zip_futures if zf.status == 'finished']:
                
                #     #gc.collect()
                # except BrokenPipeError as e:
                #     print(f'Caught BrokenPipeError: {e}')
                #     # Record the failure
                #     with open('error_log.err', 'a') as f:
                #         f.write(f'BrokenPipeError: {e}\n')
                #     # Exit gracefully
                #     sys.exit(1)
                # except Exception as e:
                #     print(f'An unexpected error occurred: {e}')
                #     # Record the failure
                #     with open('error_log.err', 'a') as f:
                #         f.write(f'Unexpected error: {e}\n')
                #     # Exit gracefully
                #     sys.exit(1)        
            # End loop if all futures are finished (or failed)
            if 'pending' not in [f.status for f in upload_futures+zul_futures+zip_futures]:
                break
    
    ####
    # Monitor upload tasks
    ####

    if failed:
        for i, failed_upload in enumerate(failed):
            print(f'Error upload {i}:\nException: {failed_upload[0]}\nTraceback: {failed_upload[1]}')

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
    parser.add_argument('--exclude', nargs='+', help="Files or folders to exclude from upload as a list in the form ['dir1', 'dir2', ...] or other valid YAML. Must relative paths to local_path.")
    parser.add_argument('--nprocs', type=int, help='Number of CPU cores to use for parallel upload.')
    parser.add_argument('--threads-per-worker', type=int, help='Number of threads per Dask worker to use for parallel upload.')
    parser.add_argument('--no-collate', default=False, action='store_true', help='Turn off collation of subfolders containing small numbers of small files into zip files.')
    parser.add_argument('--dryrun', default=False, action='store_true', help='Perform a dry run without uploading files.')
    parser.add_argument('--no-checksum', default=False, action='store_true', help='Do not perform checksum validation during upload.')
    parser.add_argument('--no-compression', default=False, action='store_true', help='Do not use compression when collating files.')
    parser.add_argument('--save-config', default=False, action='store_true', help='Save the configuration to the provided config file path and exit.')
    args = parser.parse_args()

    if not args.config_file and not (args.bucket_name and args.local_path and args.S3_prefix):
        parser.error('If a config file is not provided, the bucket name, local path, and S3 prefix must be provided.')
    if args.config_file and (args.bucket_name or args.local_path or args.S3_prefix or args.S3_folder or args.exclude or args.nprocs or args.threads_per_worker or args.no_collate or args.dryrun or args.no_checksum or args.no_compression):
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
                if 'threads_per_worker' in config.keys() and not args.threads_per_worker:
                    args.threads_per_worker = config['threads_per_worker']
                if 'threads_per_worker' not in config.keys() and not args.threads_per_worker: # required to allow default value of 4 as this overrides "default" in add_argument
                    args.threads_per_worker = 2
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
    threads_per_worker = args.threads_per_worker
    print(f'threads per worker: {threads_per_worker}')
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
            yaml.dump({
                'bucket_name': bucket_name,
                'local_path': local_dir,
                'S3_prefix': prefix,
                'S3_folder': sub_dirs,
                'exclude': exclude.to_list(),
                'nprocs': nprocs,
                'threads_per_process': threads_per_worker,
                'no_collate': not global_collate,
                'dryrun': dryrun,
                'no_checksum': not perform_checksum,
                'no_compression': not use_compression,
                }, f)
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

    ############################
    #        Dask Setup        #
    ############################
    total_memory = virtual_memory().total
    n_workers = nprocs//threads_per_worker
    mem_per_worker = virtual_memory().total//n_workers # e.g., 187 GiB / 48 * 2 = 7.8 GiB
    print(f'nprocs: {nprocs}, Threads per worker: {threads_per_worker}, Number of workers: {n_workers}, Total memory: {total_memory/1024**3:.2f} GiB, Memory per worker: {mem_per_worker/1024**3:.2f} GiB')
    client = Client(n_workers=n_workers,threads_per_worker=threads_per_worker,memory_limit=mem_per_worker) #,silence_logs=ERROR

    print(f'Dask Client: {client}', flush=True)
    current_objects = pd.DataFrame.from_dict({'CURRENT_OBJECTS':current_objects})
    print(f'Current objects: {len(current_objects)}')
    if not current_objects.empty:
        current_objects['METADATA'] = current_objects['CURRENT_OBJECTS'].apply(find_metadata, bucket=bucket)
    else:
        current_objects['METADATA'] = None

    # current_objects.to_csv('current_objects.csv', index=False)

    ## check if log exists in the bucket, and download it and append top it if it does
    # TODO: integrate this with local check for log file
    if current_objects['CURRENT_OBJECTS'].isin([log]).any():
        print(f'Log file {log} already exists in bucket. Downloading.')
        bucket.download_file(log, log)
    elif current_objects['CURRENT_OBJECTS'].isin([previous_log]).any():
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
        process_files(s3_host,access_key, secret_key, bucket_name, current_objects, exclude, local_dir, destination_dir, perform_checksum, dryrun, log, global_collate, use_compression, client, mem_per_worker)
    
    client.close()
    print(f'Dask Client closed at {datetime.now()}, elapsed time = {datetime.now() - start}')

    # Complete
    final_time = datetime.now() - start
    final_time_seconds = final_time.seconds + final_time.microseconds / 1e6
    log = pd.read_csv(log)
    log = log.drop_duplicates(subset='DESTINATION_KEY', keep='last')
    log = log.reset_index(drop=True)
    log.to_csv(log, index=False)

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
                        mem_per_worker,
                        )
    
    final_size = log["FILE_SIZE"].sum() / 1024**2
    try:
        final_transfer_speed = final_size / final_time_seconds
        
    except ZeroDivisionError:
        final_transfer_speed = 0
    try:
        final_transfer_speed_sperf = final_time_seconds / len(log)
    except ZeroDivisionError:
        final_transfer_speed_sperf = 0
    print(f'Finished at {datetime.now()}, elapsed time = {final_time}')
    print(f'Total: {len(log)} files; {(final_size):.2f} MiB; {(final_transfer_speed):.2f} MiB/s including setup time; {final_transfer_speed_sperf:.2f} s/file including setup time')
