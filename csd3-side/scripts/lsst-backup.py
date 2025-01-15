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
from itertools import repeat
from time import sleep
import warnings
from datetime import datetime, timedelta
import hashlib
import base64
import pandas as pd
from ast import literal_eval
import numpy as np
import yaml
import io
import zipfile
import warnings
from psutil import virtual_memory as mem
warnings.filterwarnings('ignore')
from logging import ERROR

import bucket_manager.bucket_manager as bm

import swiftclient

import hashlib
import os
import argparse
from dask import dataframe as dd
from dask.distributed import Client, get_client, wait, as_completed, Future, fire_and_forget
from dask.distributed import print as dprint
import subprocess

from typing import List

def compare_zip_contents(collate_objects: list[str] | pd.DataFrame, current_objects: pd.DataFrame, destination_dir: str, skipping: int) -> list[str] | pd.DataFrame:
    """
    Compare the contents of zip files to determine which files need to be uploaded.

    Parameters:
    collate_objects (list[str] | pd.DataFrame): A list of file paths or a DataFrame of file paths to be collated into zip files containing 'object_names' and 'upload' columns.
    current_objects (pd.DataFrame): A DataFrame containing metadata of current zip files, including their contents.
    destination_dir (str): The directory where the zip files will be stored.

    Returns:
    list[str] | pd.DataFrame: A list of indices of zip files to be uploaded if collate_objects is a list, or a DataFrame with an 'upload' column indicating which zip files need to be uploaded if collate_objects is a DataFrame.
    """
    if type(collate_objects) == pd.DataFrame:
        df = True
    else:
        df = False
        zips_to_upload = []

    for i in range(len(collate_objects)):
        if df:
            cmp = [x.replace(destination_dir+'/', '') for x in collate_objects['object_names'].iloc[i]]
        else:
            cmp = [x.replace(destination_dir+'/', '') for x in collate_objects[i]]
        if not current_objects.empty:
            if current_objects['METADATA'].isin([cmp]).any():
                existing_zip_contents = current_objects[current_objects['METADATA'].isin([cmp])]['METADATA'].values[0]
                if all([x in existing_zip_contents for x in cmp]):
                    print(f'Zip file {destination_dir}/collated_{i}.zip already exists and file lists match - skipping.', flush=True)
                    if df:
                        if collate_objects.iloc[i]['upload']:
                            collate_objects.iloc[i]['upload'] = False
                            skipping += 1
                    else:
                        skipping += 1
                else:
                    print(f'Zip file {destination_dir}/collated_{i}.zip already exists but file lists do not match - reuploading.', flush=True)
                    if df:
                        if not collate_objects.iloc[i]['upload']:
                            collate_objects.iloc[i]['upload'] = True
                            skipping -= 1
                    else:
                        zips_to_upload.append(i)
            else:
                print(f'Zip file {destination_dir}/collated_{i}.zip does not exist - uploading.', flush=True)
                if df:
                    if not collate_objects.iloc[i]['upload']:
                        collate_objects.iloc[i]['upload'] = True
                        skipping -= 1
                else:
                    zips_to_upload.append(i)
        else:
            print(f'Zip file {destination_dir}/collated_{i}.zip does not exist - uploading.', flush=True)
            if df:
                collate_objects.iloc[i]['upload'] = True
            else:
                zips_to_upload.append(i)
    if df:
        return collate_objects, skipping
    else:
        return zips_to_upload, skipping

def compare_zip_contents_bool(collate_object_names, id: int, current_objects: pd.DataFrame, destination_dir: str) -> bool:
    """
    Compare the contents of zip files to determine which files need to be uploaded.

    Parameters:
    collate_object: DataFrame of file paths to be collated into zip files containing 'object_names' and 'upload' columns.
    current_objects (pd.DataFrame): A DataFrame containing metadata of current zip files, including their contents.
    destination_dir (str): The directory where the zip files will be stored.
    skipping (int): The number of zip files that have been skipped.

    Returns:
    return_bool: A bool == True if the zip should be uploaded.
    """
    return_bool = True
    # dprint(f'collate_object_names: {collate_object_names}', flush=True)
    # dprint(f'type: {type(collate_object_names)}', flush=True)
    # dprint(f'len collate_object_names: {len(collate_object_names)}', flush=True)
    # dprint(f'current_objects metadata: {current_objects["METADATA"]}', flush=True)
    # dprint(f'type current_objects["METADATA"].values[362681]: {type(current_objects["METADATA"].values[362681])}', flush=True)
    # dprint(f'current_objects["METADATA"].values[362681]: {current_objects["METADATA"].values[362681]}', flush=True)
    # dprint(f'len current_objects["METADATA"].values[362681]: {len(current_objects["METADATA"].values[362681])}', flush=True)
    # dprint(f'current_objects["METADATA"].values[362681][0]: {current_objects["METADATA"].values[362681][0]}', flush=True)
    # dprint(f'type current_objects["METADATA"].values[362681][0]: {type(current_objects["METADATA"].values[362681][0])}', flush=True)
    cmp = [ x.replace(destination_dir+'/', '') for x in collate_object_names]
    # dprint(f'cmp: {cmp}', flush=True)
    # dprint(f'len cmp: {len(cmp)}', flush=True)
    if not current_objects.empty:
        # dprint(current_objects['METADATA'], flush=True)
        # dprint(f'cmp bool: {current_objects["METADATA"].isin([cmp]).any()}', flush=True)
        if current_objects['METADATA'].isin([cmp]).any():
            # dprint('in if', flush=True)
            existing_zip_contents = current_objects[current_objects['METADATA'].isin([cmp])]['METADATA'].values[0]
            # dprint(f'existing_zip_contents: {existing_zip_contents}', flush=True)
            # dprint(f'example: {existing_zip_contents}', flush=True)
            if all([x in existing_zip_contents for x in cmp]):
                dprint(f'Zip file {destination_dir}/collated_{id}.zip already exists and file lists match - skipping.', flush=True)
                return_bool = False

            else:
                dprint(f'Zip file {destination_dir}/collated_{id}.zip already exists but file lists do not match - reuploading.', flush=True)
                #if not collate_object['upload']:
                return_bool = True

        else:
            dprint(f'Zip file {destination_dir}/collated_{id}.zip does not exist - uploading.', flush=True)

            # if not collate_object['upload']:
            return_bool = True
                # skipping -= 1

    # else:
        # print(f'Zip file {destination_dir}/collated_i.zip does not exist - uploading.', flush=True)

        # return_bool = True

    # dprint(f'return bool {return_bool}', flush=True)
    # exit()
    return return_bool


def to_rds_path(home_path: str, local_dir: str) -> str:
    # get base folder for rds- folders
    if 'rds-' in local_dir:
        split = local_dir.split('/')
        for f in split:
            if f.startswith('rds-'):
                local_dir_base = '/'.join(split[:split.index(f)+1])
                break
        if 'rds-iris' in home_path:
            home_path_split = home_path.split('/')
            for hp in home_path_split:
                if hp.startswith('rds-iris'):
                    home_path_base = '/'.join(home_path_split[:home_path_split.index(hp)+1])
                    break
            rds_path = home_path.replace(home_path_base, local_dir_base)
            return rds_path
        else:
            return home_path
    else:
        return home_path

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
            print('.', end='', flush=True)
            try:
                existing_zip_contents = str(bucket.Object(''.join([key,'.metadata'])).get()['Body'].read().decode('UTF-8')).split('|') # use | as separator
            except Exception as e:
                try:
                    existing_zip_contents = bucket.Object(key).metadata['zip-contents'].split('|') # use | as separator
                except KeyError:
                    return None
                except Exception as e:
                    return None
            if existing_zip_contents:
                if len(existing_zip_contents) == 1:
                        existing_zip_contents = existing_zip_contents[0].split(',') # revert to comma if no | found
                return existing_zip_contents
        else:
            return None
    else:
        return None

def find_metadata_swift(key: str, conn, container_name: str) -> List[str]:
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
            print('.', end='', flush=True)
            try:
                existing_zip_contents = str(conn.get_object(container_name,''.join([key,'.metadata']))[1].decode('UTF-8')).split('|') # use | as separator
            except Exception as e:
                try:
                    existing_zip_contents = conn.head_object(container_name,key)['x-object-meta-zip-contents'].split('|') # use | as separator
                except KeyError:
                    return None
                except Exception as e:
                    return None
            if existing_zip_contents:
                if len(existing_zip_contents) == 1:
                        existing_zip_contents = existing_zip_contents[0].split(',') # revert to comma if no | found
                return existing_zip_contents
        else:
            return None
    else:
        return None

def mem_check(futures):
    """
    Checks the memory usage of the Dask workers.

    Args:
        futures (list): A list of Dask futures.

    Returns:
        None
    """
    client = get_client()
    workers = client.scheduler_info()['workers']
    system_perc = mem().percent
    print(f'System memory usage: {system_perc:.0f}%.', file=sys.stderr)
    min_w_mem = None
    high_mem_workers = []
    for w in workers.items():
        if min_w_mem is None or min_w_mem > w[1]['memory_limit']:
            min_w_mem = w[1]['memory_limit']
        used = w[1]['metrics']['managed_bytes'] + w[1]['metrics']['spilled_bytes']['memory']
        used_perc = used / w[1]['memory_limit'] * 100
        if used_perc > 80:
            high_mem_workers.append(w[1]['id'])
    if high_mem_workers:
        print(f'High memory usage on workers: {high_mem_workers}.', file=sys.stderr)
        client.rebalance()
        wait(futures)


def remove_duplicates(l: list[dict]) -> list[dict]:
    return pd.DataFrame(l).drop_duplicates().to_dict(orient='records')

def zip_and_upload(id, file_paths, s3, bucket_name, api, destination_dir, local_dir, total_size_uploaded, total_files_uploaded, use_compression, dryrun, processing_start, mem_per_worker) -> tuple[object, str]:
    """
    Zips a list of files and uploads the resulting zip file to an S3 bucket.
    Args:
        id (str): Identifier for the zip file.
        file_paths (list): List of file paths to be included in the zip file.
        s3 (swiftclient.Connection | None): if api == "swift": swiftclient.Connection for uploading the zip file; elif api == "s3": None.
        bucket_name (str): Name of the S3 bucket where the zip file will be uploaded.
        api (str): API name: "swift" or "s3".
        destination_dir (str): Destination "directory" in the S3 bucket.
        local_dir (str): Local directory containing the files to be zipped.
        total_size_uploaded (int): Total size of files uploaded so far.
        total_files_uploaded (int): Total number of files uploaded so far.
        use_compression (bool): Whether to use compression for the zip file.
        dryrun (bool): If True, perform a dry run without actual upload.
        processing_start (datetime): Start time of the processing.
        mem_per_worker (int): Memory allocated per worker.
    Returns:
        bool: True if a zip was created and uploaded, False if not..
    """
    print(f'Zipping and uploading {len(file_paths)} files from {local_dir} to {destination_dir}/collated_{id}.zip.', flush=True)
    #############
    #  zip part #
    #############
    client = get_client()

    zip_data, namelist = zip_folders(local_dir, file_paths, use_compression, dryrun, id, mem_per_worker)
    print('Created zipFile in memory', flush=True)
    ###############
    # upload part #
    ###############
    # zips now placed at top level of backup == local_dir
    zip_object_key = os.sep.join([destination_dir, os.path.relpath(f'{local_dir}/collated_{id}.zip', local_dir)])
    print(f'zip_object_key: {zip_object_key}', flush=True)
    if namelist == []:
        print(f'No files to upload in zip file.')
        return False
    else: # for no subtasks
        upload_and_callback(
            s3,
            bucket_name,
            api,
            local_dir,
            destination_dir,
            zip_data,
            namelist,
            zip_object_key,
            dryrun,
            processing_start,
            1, # i.e., 1 zip file
            len(zip_data),
            total_size_uploaded,
            total_files_uploaded,
            True,
            mem_per_worker
        )
        return True

def zip_folders(local_dir:str, file_paths:list[str], use_compression:bool, dryrun:bool, id:int, mem_per_worker:int) -> tuple[str, int, bytes]:
    """
    Collates the specified folders into a zip file.

    Args:
        file_paths (list): A list of lists containing files to be included in the zip file for each subfolder.
        use_compression (bool): Flag indicating whether to use compression for the zip file.
        dryrun (bool): Flag indicating whether to perform a dry run without actually creating the zip file.
        id (int, optional): An optional identifier for the zip file. Defaults to 0.
        mem_per_worker (int): The memory per worker in bytes.

    Returns:
        tuple: A tuple containing the parent folder path, the identifier, and the compressed zip file as a bytes object.

    Raises:
        None

    """
    zipped_size = 0
    if not dryrun:
        try:
            zip_buffer = io.BytesIO()
            if use_compression:
                compression = zipfile.ZIP_DEFLATED  # zipfile.ZIP_DEFLATED = standard compression
            else:
                compression = zipfile.ZIP_STORED  # zipfile.ZIP_STORED = no compression
            with zipfile.ZipFile(zip_buffer, "a", compression, True) as zip_file:
                for file in file_paths:
                    if file.startswith('/'):
                        file_path = file
                    else:
                        exit(f'Path is wrong: {file}')
                    arc_name = os.path.relpath(file_path, local_dir)
                    try:
                        zipped_size += os.path.getsize(file_path)
                        with open(file_path, 'rb') as src_file:
                            zip_file.writestr(arc_name, src_file.read())
                    except PermissionError:
                        print(f'WARNING: Permission error reading {file_path}. File will not be backed up.')
                        continue
                namelist = zip_file.namelist()
            if zipped_size > mem_per_worker:
                print(f'WARNING: Zipped size of {zipped_size} bytes exceeds memory per core of {mem_per_worker} bytes.')
        except MemoryError as e:
            print(f'Error zipping: {e}')
            print(f'Namespace: {globals()}')
            exit(1)
        if namelist == []:
            return b'', []
        return zip_buffer.getvalue(), namelist
    else:
        return b'', []

def part_uploader(bucket_name, object_key, part_number, chunk_data, upload_id) -> dict:
    """
    Uploads a part of a file to an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The key of the object in the S3 bucket.
        part_number (int): The part number of the chunk being uploaded.
        chunk_data (bytes): The data of the chunk being uploaded.
        upload_id (str): The ID of the ongoing multipart upload.

    Returns:
        dict: A dictionary containing the part number and ETag of the uploaded part.
    """
    s3_client = bm.get_client()
    return {"PartNumber":part_number,
            "ETag":s3_client.upload_part(Body=chunk_data,
                          Bucket=bucket_name,
                          Key=object_key,
                          PartNumber=part_number,
                          UploadId=upload_id)["ETag"]}

def upload_to_bucket(s3, bucket_name, api, local_dir, folder, filename, object_key, dryrun, mem_per_worker) -> str:
    """
    Uploads a file to an S3 bucket.
    Calculates a checksum for the file

    Args:
        s3 (None | swiftclient.Connection): None or swiftclient Connection object.
        bucket_name (str): The name of the S3 bucket or Swift container name.
        api (str): The API to use for the S3 connection, 's3' or 'swift'.
        folder (str): The local folder containing the file to upload.
        filename (str): The name of the file to upload.
        object_key (str): The key to assign to the uploaded file in the S3 bucket.
        dryrun (bool): Flag indicating whether to perform a dry run (no actual upload).
        mem_per_worker (int): The memory per worker in bytes.

    Returns:
        str: A string containing information about the uploaded file in CSV format.
            The format is: LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,CHECKSUM_SIZE,CHECKSUM_KEY
    """
    if api == 's3': # Need to make a new S3 connection
        s3 = bm.get_resource()
        s3_client = bm.get_client()
        bucket = s3.Bucket(bucket_name)

        # Check if the file is a symlink
        # If it is, upload an object containing the target path instead
        link = False
        if os.path.islink(filename):
            link = True
        if link:
            file_data = to_rds_path(os.path.realpath(filename), local_dir)
        else:
            file_data = open(filename, 'rb').read()

        file_size = os.path.getsize(filename)
        use_future = False

        if file_size > 10 * 1024**3:
            if not dryrun:
                print(f'WARNING: File size of {file_size} bytes exceeds memory per worker of {mem_per_worker} bytes.', flush=True)
                print('Running upload_object.py.', flush=True)
                print('This upload WILL NOT be checksummed or tracked!', flush=True)
                del file_data
                # Ensure consistent path to upload_object.py
                upload_object_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../scripts/upload_object.py')
                success = subprocess.Popen(
                    ['nohup', 'nice', '-n', '10', 'python', upload_object_path, '--bucket-name', bucket_name, '--object-name', object_key, '--local-path', filename],
                    stdout=open(f'{os.environ["PWD"]}/ext_uploads.log', 'a'),
                    stderr=open(f'{os.environ["PWD"]}/ext_uploads.err', 'a'),
                    env=os.environ,
                    preexec_fn=os.setsid
                    )
                print(f'Running upload_object.py for {filename}.', flush=True)
                return f'"{folder}","{filename}",{file_size},"{bucket_name}","{object_key}","n/a","n/a"'

        print(f'Uploading {filename} from {folder} to {bucket_name}/{object_key}, {file_size} bytes, checksum = True, dryrun = {dryrun}', flush=True)
        """
        - Upload the file to the bucket
        """
        if not dryrun:
            if link:
                """
                - Upload the link target _path_ to an object
                """
                if use_future:
                    bucket.put_object(Body=get_client().gather(file_data), Key=object_key)
                    try:
                        get_client().scatter(file_data)
                    except TypeError as e:
                        print(f'Error scattering {filename}: {e}')
                        exit(1)
                else:
                    bucket.put_object(Body=file_data, Key=object_key)
            if not link:
                """
                - Create checksum object
                """
                if use_future:
                    file_data = get_client().gather(file_data)
                checksum_hash = hashlib.md5(file_data)
                checksum_string = checksum_hash.hexdigest()
                checksum_base64 = base64.b64encode(checksum_hash.digest()).decode()
                if use_future:
                    try:
                        file_data = get_client().scatter(file_data)
                    except TypeError as e:
                        print(f'Error scattering {filename}: {e}')
                        exit(1)
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
                            part_futures.append(get_client().submit(
                            part_uploader,
                                bucket_name,
                                object_key,
                                part_number,
                                file_data[start:end],
                                mp_upload.id
                            ))
                        for future in as_completed(part_futures):
                            parts.append(future.result())
                            del future
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
                        if use_future:
                            bucket.put_object(Body=get_client().gather(file_data), Key=object_key, ContentMD5=checksum_base64)
                        else:
                            bucket.put_object(Body=file_data, Key=object_key, ContentMD5=checksum_base64)
                except Exception as e:
                    print(f'Error uploading {filename} to {bucket_name}/{object_key}: {e}')

                del file_data
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
        if link:
            return_string += ',n/a'
        else:
            return_string += f',{checksum_string}'

        #for no zip contents
        return_string += ',n/a'
        return return_string
    elif api == 'swift':
        try:
            assert type(s3) is swiftclient.Connection
        except AssertionError:
            raise AssertionError('s3_host must be a swiftclient.Connection object.')

        filename = object_key.split('/')[-1]

        # Check if the file is a symlink
        # If it is, upload an object containing the target path instead
        link = False
        if os.path.islink(filename):
            link = True
        if link:
            file_data = to_rds_path(os.path.realpath(filename), local_dir)
        else:
            file_data = open(filename, 'rb').read()

        file_size = os.path.getsize(filename)
        use_future = False

        print(f'Uploading {filename} from {folder} to {bucket_name}/{object_key}, {file_size} bytes, checksum = True, dryrun = {dryrun}', flush=True)
        """
        - Upload the file to the bucket
        """
        if not dryrun:
            if link:
                """
                - Upload the link target _path_ to an object
                """
                s3.put_object(container=bucket_name, obj=object_key, contents=file_data)
            if not link:
                """
                - Create checksum object
                """
                checksum_hash = hashlib.md5(file_data)
                checksum_string = checksum_hash.hexdigest()
                checksum_base64 = base64.b64encode(checksum_hash.digest()).decode()

                try:
                    if file_size > mem_per_worker or file_size > 5 * 1024**3:  # Check if file size is larger than 5GiB
                        """
                        - Use multipart upload for large files
                        """
                        swift_service = bm.get_swift_service()
                        segment_size = 512*1024**2
                        segments = []
                        n_segments = int(np.ceil(file_size / segment_size))
                        for i in range(n_segments):
                            start = i * segment_size
                            end = min(start + segment_size, file_size)
                            segments.append(file_data[start:end])
                        segment_objects = [bm.get_SwiftUploadObject(filename, object_name=object_key, options={'contents':segment, 'content_type':None}) for segment in segments]
                        segmented_upload = [filename]
                        for so in segment_objects:
                            segmented_upload.append(so)
                        print(f'Uploading {filename} to {bucket_name}/{object_key} in {n_segments} parts.', flush=True)
                        results = swift_service.upload(
                            bucket_name,
                            segmented_upload,
                            options={
                                'meta': [],
                                'header': [],
                                'segment_size': segment_size,
                                'use_slo': True,
                                'segment_container': bucket_name+'-segments'
                            }
                        )
                    else:
                        """
                        - Upload the file to the bucket
                        """
                        print(f'Uploading {filename} to {bucket_name}/{object_key}')
                        s3.put_object(container=bucket_name, contents=file_data, content_type='multipart/mixed', obj=object_key, etag=checksum_string)
                except Exception as e:
                    print(f'Error uploading {filename} to {bucket_name}/{object_key}: {e}')

                del file_data
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
        if link:
            return_string += ',n/a'
        else:
            return_string += f',{checksum_string}'

        #for no zip contents
        return_string += ',n/a'
        return return_string

def upload_to_bucket_collated(s3, bucket_name, api, folder, file_data, zip_contents, object_key, dryrun, mem_per_worker) -> str:
    """
    Uploads a file to an S3 bucket.
    Calculates a checksum for the file

    Args:
        s3 (None | swiftclient.Connection): None or Swift swiftclient.Connection object.
        bucket_name (str): The name of the S3 bucket.
        api (str): The API to use for the S3 connection, 's3' or 'swift'.
        folder (str): The local folder containing the file to upload.
        file_data (bytes): The file data to upload (zipped).
        zip_contents (list): A list of files included in the zip file (file_data).
        object_key (str): The key to assign to the uploaded file in the S3 bucket.
        dryrun (bool): Flag indicating whether to perform a dry run (no actual upload).
        mem_per_worker (int): memory limit of each Dask worker.

    Returns:
        str: A string containing information about the uploaded file in CSV format.
            The format is: LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,CHECKSUM_SIZE,CHECKSUM_KEY
    """
    if api == 's3':
        s3 = bm.get_resource()
        bucket = s3.Bucket(bucket_name)

        filename = object_key.split('/')[-1]
        file_data_size = len(file_data)

        print(f'Uploading zip file "{filename}" for {folder} to {bucket_name}/{object_key}, {file_data_size} bytes, checksum = True, dryrun = {dryrun}', flush=True)
        """
        - Upload the file to the bucket
        """
        if not dryrun:
            """
            - Create checksum object
            """
            checksum_hash = hashlib.md5(file_data)
            checksum_string = checksum_hash.hexdigest()
            checksum_base64 = base64.b64encode(checksum_hash.digest()).decode()

            try:
                """
                - Upload the file to the bucket
                """
                print(f'Uploading zip file "{filename}" ({file_data_size} bytes) to {bucket_name}/{object_key}')
                metadata_value = '|'.join(zip_contents) # use | as separator

                metadata_object_key = object_key + '.metadata'
                print(f'Writing zip contents to {metadata_object_key}.', flush=True)
                bucket.put_object(Body=metadata_value, Key=metadata_object_key, Metadata={'corresponding-zip': object_key})
                metadata = {'zip-contents-object': metadata_object_key}

                bucket.put_object(Body=file_data, Key=object_key, ContentMD5=checksum_base64, Metadata=metadata)
            except Exception as e:
                print(f'Error uploading "{filename}" ({file_data_size}) to {bucket_name}/{object_key}: {e}')
                exit(1)
        else:
            checksum_string = "DRYRUN"

        """
            report actions
            CSV formatted
            header: LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,ZIP_CONTENTS
        """
        return_string = f'"{folder}","{filename}",{file_data_size},"{bucket_name}","{object_key}","{checksum_string}","{",".join(zip_contents)}"'

        return return_string

    elif api == 'swift':
        try:
            assert type(s3) is swiftclient.Connection
        except AssertionError:
            raise AssertionError('s3_host must be a swiftclient.Connection object.')

        filename = object_key.split('/')[-1]
        file_data_size = len(file_data)

        print(f'Uploading zip file "{filename}" for {folder} to {bucket_name}/{object_key}, {file_data_size} bytes, checksum = True, dryrun = {dryrun}', flush=True)
        """
        - Upload the file to the bucket
        """
        if not dryrun:
            """
            - Create checksum object
            """
            checksum_hash = hashlib.md5(file_data)
            checksum_string = checksum_hash.hexdigest()
            checksum_base64 = base64.b64encode(checksum_hash.digest()).decode()

            try:

                """
                - Upload the file to the bucket
                """
                print(f'Uploading zip file "{filename}" ({file_data_size} bytes) to {bucket_name}/{object_key}')
                metadata_value = '|'.join(zip_contents) # use | as separator

                metadata_object_key = object_key + '.metadata'
                print(f'Writing zip contents to {metadata_object_key}.', flush=True)
                responses = [{},{}]
                s3.put_object(container=bucket_name, contents=metadata_value, content_type='text/plain', obj=metadata_object_key, headers={'x-object-meta-corresponding-zip': object_key}, response_dict=responses[0])
                #bucket.put_object(Body=file_data, Key=object_key, ContentMD5=checksum_base64, Metadata=metadata)
                s3.put_object(container=bucket_name, contents=file_data, content_type='multipart/mixed', obj=object_key, etag=checksum_string, headers={'x-object-meta-zip-contents-object':metadata_object_key}, response_dict=responses[1])
            except Exception as e:
                print(f'Error uploading "{filename}" ({file_data_size}) to {bucket_name}/{object_key}: {e}')
                exit(1)
        else:
            checksum_string = "DRYRUN"

        """
            report actions
            CSV formatted
            header: LOCAL_FOLDER,LOCAL_PATH,FILE_SIZE,BUCKET_NAME,DESTINATION_KEY,CHECKSUM,ZIP_CONTENTS
        """
        return_string = f'"{folder}","{filename}",{file_data_size},"{bucket_name}","{object_key}","{checksum_string}","{",".join(zip_contents)}"'
        while True:
            if responses[0] and responses[1]:
                if responses[0]['status'] == 201 and responses[1]['status'] == 201:
                    break

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
    # pass

    # This give false information as it is called once per file, not once per folder.
    # print('#######################################')
    # print('THIS INFORMATION IS CURRENTLY INCORRECT')

    elapsed = file_end - file_start
    if collated:
        print(f'Uploaded zip file, elapsed time = {elapsed}')
    else:
        print(f'Uploaded {file_name_or_data}, elapsed time = {elapsed}')
    try:
        elapsed_seconds = elapsed.seconds + elapsed.microseconds / 1e6
        avg_file_size = total_size / file_count / 1024**2
        print(f'{file_count} files (avg {avg_file_size:.2f} MiB/file) uploaded in {elapsed_seconds:.2f} seconds, {elapsed_seconds/file_count:.2f} s/file', flush=True)
        print(f'{total_size / 1024**2:.2f} MiB uploaded in {elapsed_seconds:.2f} seconds, {total_size / 1024**2 / elapsed_seconds:.2f} MiB/s', flush=True)
        # print(f'Total elapsed time = {file_end-processing_start}', flush=True)
        # print(f'Total files uploaded = {total_files_uploaded}', flush=True)
        # print(f'Total size uploaded = {total_size_uploaded / 1024**3:.2f} GiB', flush=True)
        # print(f'Running average speed = {total_size_uploaded / 1024**2 / (file_end-processing_start).seconds:.2f} MiB/s', flush=True)
        # print(f'Running average rate = {(file_end-processing_start).seconds / total_files_uploaded:.2f} s/file', flush=True)
        # print('END OF INCORRENT REPORTING')
        # print('#######################################')
    except ZeroDivisionError:
        pass

def upload_and_callback(s3, bucket_name, api, local_dir, folder, file_name_or_data, zip_contents, object_key, dryrun, processing_start, file_count, folder_files_size, total_size_uploaded, total_files_uploaded, collated, mem_per_worker) -> None:
    """
    Uploads files to an S3 bucket and logs the output. Supports both collated (zipped) and individual file uploads.
    Args:
        s3 (None | swiftclient.Connection): The S3 host URL or swiftclient.Connection
        bucket_name (str): The name of the S3 bucket.
        api (str): The API object for interacting with S3.
        local_dir (str): The local directory containing the files to upload.
        folder (str): The folder containing the files to upload.
        file_name_or_data (str or bytes): The name of the file or the file data to upload.
        zip_contents (bool): Whether the contents should be zipped before uploading.
        object_key (str): The object key for the uploaded file in the S3 bucket.
        dryrun (bool): If True, perform a dry run without actual upload.
        processing_start (datetime): The start time of the processing.
        file_count (int): The number of files to upload.
        folder_files_size (int): The total size of the files in the folder.
        total_size_uploaded (int): The total size of the files uploaded so far.
        total_files_uploaded (int): The total number of files uploaded so far.
        collated (bool): If True, upload files as a single zipped file.
        mem_per_worker (int): The memory allocated per worker for the upload process.
    Returns:
        None
    """
    # upload files in parallel and log output
    file_start = datetime.now()
    print(f'collated = {collated}', flush=True)
    if collated:
        try:
            print(f'Uploading zip containing {file_count} subfolders from {folder}.')
            result = upload_to_bucket_collated(s3, bucket_name, api, folder, file_name_or_data, zip_contents, object_key, dryrun, mem_per_worker)
        except Exception as e:
            print(f'Error uploading {folder} to {bucket_name}/{object_key}: {e}')
            sys.exit(1)
    else:
        print(f'Uploading {file_count} files from {folder}.')
        result = upload_to_bucket(s3, bucket_name, api, local_dir, folder, file_name_or_data, object_key, dryrun, mem_per_worker)

    file_end = datetime.now()
    print_stats(file_name_or_data, file_count, folder_files_size, file_start, file_end, processing_start, total_size_uploaded, total_files_uploaded, collated)
    with open(log, 'a') as logfile:
        logfile.write(f'{result}\n')

    del file_name_or_data

    return None

### KEY FUNCTION TO FIND ALL FILES AND ORGANISE UPLOADS ###
def process_files(s3, bucket_name, api, current_objects, exclude, local_dir, destination_dir, dryrun, log, global_collate, use_compression, client, mem_per_worker, collate_list_file, save_collate_file, file_count_stop) -> None:
    """
    Uploads files from a local directory to an S3 bucket in parallel.

    Args:
        s3 (None or swiftclient.Connection): None or swiftclient.Connection.
        bucket_name (str): The name of the S3 bucket.
        api (str): The API to use for the S3 connection, 's3' or 'swift'.
        current_objects (ps.Dataframe): A list of object names already present in the S3 bucket.
        local_dir (str): The local directory containing the files to upload.
        destination_dir (str): The destination directory in the S3 bucket.
        dryrun (bool): Flag indicating whether to perform a dry run without actually uploading the files.
        log (str): The path to the log file.
        global_collate (bool): Flag indicating whether to collate files into zip files before uploading.
        use_compression (bool): Flag indicating whether to use compression for the zip files.
        client (dask.Client): The Dask client object.
        mem_per_worker (int): The memory per worker in bytes.
        collate_list_file (str): The path to the file containing a list of dicts describing files and folders to collate.
        save_collate_file (bool): Save (possibly overwrite) the collate_list_file.

    Returns:
        None
    """
    if api == 's3':
        try:
            assert s3 == None
        except AssertionError:
            raise AssertionError('s3 must be None if using S3 API.')
    elif api == 'swift':
        try:
            assert type(s3) is swiftclient.Connection
        except AssertionError:
            raise AssertionError('s3 must be a swiftclient.Connection object if using Swift API.')
    processing_start = datetime.now()
    total_size_uploaded = 0
    total_files_uploaded = 0
    i = 0

    #recursive loop over local folder
    to_collate_list = [] # to be used for storing file lists to be collated as list of dicts
    total_all_folders = 0
    total_all_files = 0
    folder_num = 0
    file_num = 0
    uploads = []
    upload_futures = []
    zul_futures = []
    failed = []
    max_zip_batch_size = 512*1024**2
    zip_batch_files = [[]]
    zip_batch_object_names = [[]]
    zip_batch_sizes = [0]

    print(f'Analysing local dataset {local_dir}.', flush=True)
    for folder, sub_folders, files in os.walk(local_dir, topdown=True):
        total_all_folders += 1
        total_all_files += len(files)
        if total_all_folders % 1000 == 0:
            print('.', end='', flush=True)
    print()
    print(f'Folders: {total_all_folders} Files: {total_all_files}', flush=True)
    if file_count_stop and len(current_objects) > 0:
        total_non_collate_zip = len(current_objects[current_objects['CURRENT_OBJECTS'].str.contains('collated_') == False])
        if total_non_collate_zip == total_all_files:
            print(f'Number of existing objects (excluding collated zips) equal to number of local files given the same prefix ({total_all_files}).')
            print('This is a soft verification that the entire local dataset has been uploaded previously.')
            print('Exiting. To prevent this behavior and force per-file verification, set `--no-file-count-stop` to True.', flush=True)
            sys.exit()
    if not os.path.exists(collate_list_file):
        print(f'Preparing to upload {total_all_files} files in {total_all_folders} folders from {local_dir} to {bucket_name}/{destination_dir}.', flush=True)
        for folder, sub_folders, files in os.walk(local_dir, topdown=False):
            folder_num += 1
            file_num += len(files)
            print(f'Processing {folder_num}/{total_all_folders} folders; {file_num}/{total_all_files} files in {local_dir}.', flush=True)

            # check if folder is in the exclude list
            if len(files) == 0 and len(sub_folders) == 0:
                print(f'Skipping subfolder - no files or subfolders.', flush=True)
                continue
            elif len(files) == 0:
                print(f'Skipping subfolder - no files.', flush=True)
                continue
            if exclude.isin([folder]).any():
                print(f'Skipping subfolder {folder} - excluded.', flush=True)
                continue
            # remove subfolders in exclude list
            if len(sub_folders) > 0:
                len_pre_exclude = len(sub_folders)
                sub_folders[:] = [sub_folder for sub_folder in sub_folders if not exclude.isin([sub_folder]).any()]
                print(f'Skipping {len_pre_exclude - len(sub_folders)} subfolders in {folder} - excluded. {len(sub_folders)} subfolders remaining.', flush=True)

            folder_files = [os.sep.join([folder, filename]) for filename in files]

            sizes = []
            for f in zul_futures+upload_futures:
                if isinstance(f, Future):
                    if f.status == 'finished':
                        del f
            for filename in folder_files:
                if exclude.isin([os.path.relpath(filename, local_dir)]).any():
                    print(f'Skipping file {filename} - excluded.', flush=True)
                    folder_files.remove(filename)
                    if len(folder_files) == 0:
                        print(f'Skipping subfolder - no files - see exclusions.', flush=True)
                    continue
                try:
                    sizes.append(os.stat(filename).st_size)
                except PermissionError:
                    print(f'WARNING: Permission error reading {filename}. File will not be backed up.', flush=True)
                    try:
                        folder_files.remove(filename)
                    except ValueError:
                        pass
                    if len(folder_files) == 0:
                        print(f'Skipping subfolder - no files - see permissions warning(s).', flush=True)
                        continue
            total_filesize = sum(sizes)
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
                    subfiles_sizes = []
                    for filename in subfolder_files:
                        try:
                            subfiles_sizes.append(os.stat(filename).st_size)
                        except PermissionError:
                            print(f'WARNING: Permission error reading {filename}. File will not be backed up.', flush=True)
                            subfolder_files.remove(filename)
                            if len(subfolder_files) == 0:
                                print(f'Skipping subfolder - no files - see permissions warning(s).', flush=True)
                                continue
                    total_subfilesize = sum(subfiles_sizes)
                    if not sub_sub_folders and len(sub_files) < 4 and total_subfilesize < 96*1024**2:
                        sub_folders.remove(sub_folder) # not sure what the effect of this is
                        # upload files in subfolder "as is" i.e., no zipping

            # check folder isn't empty
            print(f'Processing {len(folder_files)} files (total size: {total_filesize/1024**2:.0f} MiB) in {folder} with {len(sub_folders)} subfolders.', flush=True)

            # keys to files on s3
            object_names = [os.sep.join([destination_dir, os.path.relpath(filename, local_dir)]) for filename in folder_files]
            init_len = len(object_names)

            if not current_objects.empty:
                if set(object_names).issubset(current_objects['CURRENT_OBJECTS']):
                    #all files in this subfolder already in bucket
                    print(f'Skipping subfolder - all files exist.', flush=True)
                    continue


            if mean_filesize > max_zip_batch_size or not global_collate:
                print('Individual upload.', flush=True)
                # all files within folder
                # if uploading file individually, remove existing files from object_names
                if not current_objects.empty:
                    for oni, on in enumerate(object_names):
                        if current_objects['CURRENT_OBJECTS'].isin([on]).any() or current_objects['CURRENT_OBJECTS'].isin([f'{on}.symlink']).any():
                            object_names.remove(on)
                            del folder_files[oni]
                pre_linkcheck_file_count = len(object_names)
                if init_len - pre_linkcheck_file_count > 0:
                    print(f'Skipping {init_len - pre_linkcheck_file_count} existing files.', flush=True)
                #always do this AFTER removing "current_objects" to avoid re-uploading
                symlink_targets = []
                symlink_obj_names = []
                for i in range(len(folder_files)):
                    if os.path.islink(folder_files[i]):
                        #rename link in object_names
                        symlink_obj_name = object_names[i]
                        object_names[i] = '.'.join([object_names[i], 'symlink'])
                        #add symlink target to symlink_targets list
                        #using target dir as-is can cause permissions issues
                        #replace /home path with /rds path uses as local_dir
                        target = to_rds_path(os.path.realpath(folder_files[i]), local_dir)
                        symlink_targets.append(target)
                        #add real file to symlink_obj_names list
                        symlink_obj_names.append(symlink_obj_name)

                folder_files.extend(symlink_targets)
                object_names.extend(symlink_obj_names)

                file_count = len(object_names)
                folder_files_size = np.sum(np.array([os.stat(filename).st_size for filename in folder_files]))
                total_size_uploaded += folder_files_size
                total_files_uploaded += file_count
                print(f'{file_count - pre_linkcheck_file_count} symlinks replaced with files. Symlinks renamed to <filename>.symlink', flush=True)

                print(f'Sending {file_count} files (total size: {folder_files_size/1024**2:.0f} MiB) in {folder} to S3 bucket {bucket_name}.', flush=True)
                print(f'Individual files objects names: {object_names}', flush=True)

                try:
                    for i,args in enumerate(zip(
                            repeat(s3),
                            repeat(api),
                            repeat(bucket_name),
                            repeat(local_dir),
                            repeat(folder),
                            folder_files,
                            repeat(None),
                            object_names,
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

                # release block of files if the list for results is greater than 4 times the number of processes

            elif len(folder_files) > 0 and global_collate: # small files in folder
                print('Collated upload.', flush=True)
                if not os.path.exists(collate_list_file):
                    # Existing object removal
                    if not current_objects.empty:
                        for oni, on in enumerate(object_names):
                            if current_objects['CURRENT_OBJECTS'].isin([on]).any() or current_objects['CURRENT_OBJECTS'].isin([f'{on}.symlink']).any():
                                object_names.remove(on)
                                del folder_files[oni]

                    pre_linkcheck_file_count = len(object_names)
                    if init_len - pre_linkcheck_file_count > 0:
                        print(f'Skipping {init_len - pre_linkcheck_file_count} existing files.', flush=True)

                    symlink_targets = []
                    symlink_obj_names = []
                    for i in range(len(folder_files)):
                        if os.path.islink(folder_files[i]):
                            #rename link in object_names
                            symlink_obj_name = object_names[i]
                            object_names[i] = '.'.join([object_names[i], 'symlink'])
                            #add symlink target to symlink_targets list
                            #using target dir as-is can cause permissions issues
                            #replace /home path with /rds path uses as local_dir
                            target = to_rds_path(os.path.realpath(folder_files[i]), local_dir)
                            symlink_targets.append(target)
                            #add real file to symlink_obj_names list
                            symlink_obj_names.append(symlink_obj_name)

                    # append symlink_targets and symlink_obj_names to folder_files and object_names
                    folder_files.extend(symlink_targets)
                    object_names.extend(symlink_obj_names)

                    file_count = len(object_names)
                    #always do this AFTER removing "current_objects" to avoid re-uploading

                    # Level n collation
                    size = zip_batch_sizes[-1]
                    print(f'Size: {size}')
                    for i, filename in enumerate(folder_files):
                        s = os.lstat(filename).st_size
                        size += s
                        if size <= max_zip_batch_size:
                            zip_batch_files[-1].append(filename)
                            zip_batch_object_names[-1].append(object_names[i])
                            zip_batch_sizes[-1] += size
                        else:
                            zip_batch_files.append([filename])
                            zip_batch_object_names.append([object_names[i]])
                            zip_batch_sizes.append(s)
                            size = s

                    folder_files_size = np.sum(np.array([os.lstat(filename).st_size for filename in folder_files]))
                    print(f'Number of zip files: {len(zip_batch_files)}', flush=True)
            print(f'Done traversing {local_dir}.', flush=True)

    if global_collate:
        ###############################
        # CHECK HERE FOR ZIP CONTENTS #
        ###############################
        if not os.path.exists(collate_list_file):
            zips_to_upload, skipping = compare_zip_contents(zip_batch_object_names, current_objects, destination_dir, 0)

            # Create dict for zip files
            for i in range(len(zip_batch_files)):
                if i in zips_to_upload:
                    to_collate_list.append(
                        {
                            'id': i,
                            'object_names': zip_batch_object_names[i],
                            'file_paths': zip_batch_files[i],
                            'size': zip_batch_sizes[i],
                            'upload': True
                        }
                    )
                else:
                    to_collate_list.append(
                        {
                            'id': i,
                            'object_names': zip_batch_object_names[i],
                            'file_paths': zip_batch_files[i],
                            'size': zip_batch_sizes[i],
                            'upload': False
                        }
                    )

            to_collate = pd.DataFrame.from_dict(to_collate_list)
            client.scatter(to_collate)
            del zip_batch_files, zip_batch_object_names, zip_batch_sizes

        else:
            if not current_objects.empty:
                # nparts = len(client.scheduler_info()['workers'])*10
                # Pandas
                to_collate = pd.read_csv(collate_list_file).drop('upload', axis=1)
                to_collate.object_names = to_collate.object_names.apply(literal_eval)
                to_collate.file_paths = to_collate.file_paths.apply(literal_eval)
                # Dask
                # to_collate = dd.from_pandas(pd.read_csv(collate_list_file).drop('upload', axis=1), npartitions=nparts)
                # to_collate.object_names = dd.from_pandas(to_collate.object_names.compute().apply(literal_eval),npartitions=nparts)
                # to_collate.file_paths = dd.from_pandas(to_collate.file_paths.compute().apply(literal_eval),npartitions=nparts)
                print(f'Loaded collate list from {collate_list_file}.', flush=True)
                # now using pandas for both current_objects and to_collate - this could be re-written to using vectorised operations
                # client.scatter([current_objects,to_collate])
                ids = to_collate['id'].copy()
                # to_collate = dd.from_pandas(to_collate, npartitions=len(client.scheduler_info()['workers'])*2)
                # print('Created Dask dataframe for to_collate.', flush=True)
                print('Created Pandas dataframe for to_collate.', flush=True)

                # print(to_collate.index)
                # print(to_collate.columns)
                # print(to_collate.dtypes)
                # client.scatter(to_collate)
                print('Scattered dataframe to distributed memory.', flush=True)
                # to_collate = to_collate.compute()

                comp_futures = []
                # dprint(to_collate[to_collate.id == 0]['object_names'].values[0])
                # for i, on in enumerate(to_collate['object_names']):
                dprint('Comparing existing zips to collate list.', flush=True)
                for id in ids:
                # for i,args in enumerate(zip(
                    # to_collate['object_names'],
                    # [i for i in range(len(to_collate))],
                    # repeat(current_objects),
                    # repeat(destination_dir),
                    # )):
                    # print(f'len to_collate on id {id}: {len(to_collate[to_collate.id == id]["object_names"].values[0])}')
                    ons = to_collate[to_collate.id == id]['object_names'].values[0]
                    comp_futures.append(client.submit(
                        compare_zip_contents_bool,
                        ons,
                        id,
                        current_objects,
                        destination_dir,
                        ))
                wait(comp_futures)
                to_collate = to_collate.compute()
                to_collate['upload'] = [f.result() for f in comp_futures]
                # print(to_collate)
                # exit()
            else:
                pass

        if save_collate_file:
            print(f'Saving collate list to {collate_list_file}, len={len(to_collate)}.', flush=True)
            to_collate.to_csv(collate_list_file, index=False)
        else:
            print(f'Collate list not saved.', flush=True)

    if len(to_collate) > 0:
        # call zip_folder in parallel
        print(f'Zipping {len(to_collate)} batches.', flush=True)
        print(to_collate[to_collate.upload == False][['file_paths','id', 'upload']])
        print(f'len(to_collate[to_collate.upload == False]): {len(to_collate[to_collate.upload == False])}, skipping: {skipping}')
        to_collate_uploads = to_collate[to_collate.upload == True][['file_paths','id', 'upload']]
        print(f'to_collate: {to_collate}, {len(to_collate)}')
        print(f'to_collate_uploads: {to_collate_uploads}, {len(to_collate_uploads)}')
        assert to_collate_uploads['upload'].all()
        for id in to_collate_uploads['id']:
            if len(upload_futures) >= len(client.scheduler_info()['workers'])*2:
                while len(upload_futures) >= len(client.scheduler_info()['workers'])*2:
                    for ulf in upload_futures:
                        if 'exception' in ulf.status or 'error' in ulf.status:
                            f_tuple = ulf.exception(), ulf.traceback()
                            failed.append(f_tuple)
                            upload_futures.remove(ulf)
                        else:
                            upload_futures.remove(ulf)
                            to_collate.loc[to_collate['id'] == id, 'upload'] = False

            upload_futures.append(client.submit(
                zip_and_upload,
                id,
                to_collate_uploads[to_collate_uploads.id == id]['file_paths'].values[0],
                s3,
                bucket_name,
                api,
                destination_dir,
                local_dir,
                total_size_uploaded,
                total_files_uploaded,
                use_compression,
                dryrun,
                processing_start,
                mem_per_worker,
            ))

    ########################
    # Monitor upload tasks #
    ########################

    for ulf in as_completed(upload_futures):
        if 'exception' in ulf.status or 'error' in ulf.status:
            f_tuple = ulf.exception(), ulf.traceback()
            failed.append(f_tuple)
            upload_futures.remove(ulf)
        elif ulf.done():
            upload_futures.remove(ulf)
            to_collate.loc[to_collate['id'] == id, 'upload'] = False
        if len(upload_futures) == 0:
            print('All zip uploads complete.', flush=True)

    if failed:
        for i, failed_upload in enumerate(failed):
            print(f'Error upload {i}:\nException: {failed_upload[0]}\nTraceback: {failed_upload[1]}', flush=True)

    # Re-save collate list to reflect uploads
    if save_collate_file:
        print(f'Saving updated collate list to {collate_list_file}.', flush=True)
        to_collate.to_csv(collate_list_file, index=False)
    else:
        print(f'Collate list not saved.')

##########################################
#              Main Function             #
##########################################
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
    parser.add_argument('--api', type=str, help='API to use; "S3" or "Swift". Case insensitive.')
    parser.add_argument('--bucket-name', type=str, help='Name of the S3 bucket.')
    parser.add_argument('--local-path', type=str, help='Absolute path to the folder to be uploaded.')
    parser.add_argument('--S3-prefix', type=str, help='Prefix to be used in S3 object keys.')
    parser.add_argument('--S3-folder', type=str, help='Subfolder(s) at the end of the local path to be used in S3 object keys.', nargs='?', const='', default='')
    parser.add_argument('--exclude', nargs='+', help="Files or folders to exclude from upload as a list in the form ['dir1', 'dir2', ...] or other valid YAML. Must relative paths to local_path.")
    parser.add_argument('--nprocs', type=int, help='Number of CPU cores to use for parallel upload.')
    parser.add_argument('--threads-per-worker', type=int, help='Number of threads per Dask worker to use for parallel upload.')
    parser.add_argument('--no-checksum', default=False, action='store_true', help='DEPRECATED - Turn off file checksum.')
    parser.add_argument('--no-collate', default=False, action='store_true', help='Turn off collation of subfolders containing small numbers of small files into zip files.')
    parser.add_argument('--dryrun', default=False, action='store_true', help='Perform a dry run without uploading files.')
    parser.add_argument('--no-compression', default=False, action='store_true', help='Do not use compression when collating files.')
    parser.add_argument('--save-config', default=False, action='store_true', help='Save the configuration to the provided config file path and exit.')
    parser.add_argument('--no-file-count-stop', default=False, action='store_true', help='Do not stop if count of local files equals count of S3 objects.')
    args = parser.parse_args()

    if not args.config_file and not (args.bucket_namse and args.local_path and args.S3_prefix):
        parser.error('If a config file is not provided, the bucket name, local path, and S3 prefix must be provided.')
    if args.config_file and (args.api or
                             args.bucket_name or
                             args.local_path or
                             args.S3_prefix or
                             args.S3_folder or
                             args.exclude or
                             args.nprocs or
                             args.threads_per_worker or
                             args.no_collate or
                             args.dryrun or
                             args.no_compression or
                             args.save_config or
                             args.no_file_count_stop):
        print(f'WARNING: Options provide on command line override options in {args.config_file}.')
    if args.config_file:
        config_file = args.config_file
        if not os.path.exists(config_file) and not args.save_config:
            sys.exit(f'Config file {config_file} does not exist.')
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
                print(config)
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
                if 'no_checksum' in config.keys() and not args.no_checksum:
                    args.no_checksum = config['no_checksum']
                if 'no_collate' in config.keys() and not args.no_collate:
                    args.no_collate = config['no_collate']
                if 'dryrun' in config.keys() and not args.dryrun:
                    args.dryrun = config['dryrun']
                if 'no_compression' in config.keys() and not args.no_compression:
                    args.no_compression = config['no_compression']
                if 'no_file_count_stop' in config.keys() and not args.no_file_count_stop:
                    args.no_file_count_stop = config['no_file_count_stop']
                if 'api' in config.keys() and not args.api:
                    args.api = config['api']
                if 'api' not in config.keys() and not args.api:
                    args.api = 's3'

    if args.save_config and not args.config_file:
        parser.error('A config file must be provided to save the configuration.')

    no_checksum = args.no_checksum
    if no_checksum:
        parser.error('Please note: the optin to disable file checksum has been deprecated.')

    save_config = args.save_config
    api = args.api.lower()
    if api not in ['s3', 'swift']:
        parser.error('API must be "S3" or "Swift" (case insensitive).')
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
    dryrun = args.dryrun
    use_compression = not args.no_compression # internally, flag turns *on* compression, but for user no-compression turns it off - makes flag more intuitive

    file_count_stop = not args.no_file_count_stop  # internally, flag turns *on* file-count-stop, but for user no-file-count-stop turns it off - makes flag more intuitive

    if args.exclude:
        exclude = pd.Series(args.exclude)
    else:
        exclude = pd.Series([])

    print(f'Config: {args}')

    if save_config:
        with open(config_file, 'w') as f:
            yaml.dump({
                'bucket_name': bucket_name,
                'api': api,
                'local_path': local_dir,
                'S3_prefix': prefix,
                'S3_folder': sub_dirs,
                'nprocs': nprocs,
                'threads_per_process': threads_per_worker,
                'no_collate': not global_collate,
                'dryrun': dryrun,
                'no_compression': not use_compression,
                'no_file_count_stop': not file_count_stop,
                'exclude': exclude.to_list(),
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

    if global_collate:
        collate_list_suffix = 'collate-list.csv'
        collate_list_file = log.replace(log_suffix,collate_list_suffix) # now automatically generated
        save_collate_list = True # no longer optional
        if save_collate_list and not os.path.exists(collate_list_file):
            print(f'Collate list will be generated and saved to {collate_list_file}.')
        elif save_collate_list and os.path.exists(collate_list_file):
            print(f'Collate list will be read from and re-saved to {collate_list_file}.')

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
    try:
        if bm.check_keys(api):
            if api == 's3':
                access_key = os.environ['S3_ACCESS_KEY']
                secret_key = os.environ['S3_ACCESS_KEY']
                s3_host = os.environ['S3_HOST_URL']
            elif api == 'swift':
                access_key = os.environ['ST_USER']
                secret_key = os.environ['ST_KEY']
                s3_host = os.environ['ST_AUTH']
    except AssertionError as e:
        print(f'AssertionError {e}', file=sys.stderr)
        sys.exit()
    except KeyError as e:
        print(f'KeyError {e}', file=sys.stderr)
        sys.exit()
    except ValueError as e:
        print(f'ValueError {e}', file=sys.stderr)
        sys.exit()

    print(f'Using {api.capitalize()} API with host {s3_host}')

    if api == 's3':
        s3 = bm.get_resource()
        bucket_list = bm.bucket_list(s3)
    elif api == 'swift':
        s3 = bm.get_conn_swift()
        bucket_list = bm.bucket_list_swift(s3)

    if bucket_name not in bucket_list:
        if not dryrun:
            if api == 's3':
                s3.create_bucket(Bucket=bucket_name)
            elif api == 'swift':
                s3.put_container(bucket_name)
            print(f'Added bucket: {bucket_name}')
    else:
        if not dryrun:
            print(f'Bucket exists: {bucket_name}')
            print('Existing files will be skipped.')
        else:
            print(f'Bucket exists: {bucket_name}')
            print('dryrun == True, so continuing.')

    if api == 's3':
        bucket = s3.Bucket(bucket_name)
    elif api == 'swift':
        bucket = None

    success = False


    ############################
    #        Dask Setup        #
    ############################
    total_memory = mem().total
    n_workers = nprocs//threads_per_worker
    mem_per_worker = mem().total//n_workers # e.g., 187 GiB / 48 * 2 = 7.8 GiB
    print(f'nprocs: {nprocs}, Threads per worker: {threads_per_worker}, Number of workers: {n_workers}, Total memory: {total_memory/1024**3:.2f} GiB, Memory per worker: {mem_per_worker/1024**3:.2f} GiB')

    # Process the files
    with Client(n_workers=n_workers,threads_per_worker=threads_per_worker,memory_limit=mem_per_worker) as client:
        print(f'Dask Client: {client}', flush=True)
        print(f'Dashboard: {client.dashboard_link}', flush=True)
        print(f'Starting processing at {datetime.now()}, elapsed time = {datetime.now() - start}')
        print(f'Using {nprocs} processes.')
        print(f'Getting current object list for {bucket_name}. This may take some time.\nStarting at {datetime.now()}, elapsed time = {datetime.now() - start}', flush=True)

        if api == 's3':
            current_objects = bm.object_list(bucket, prefix=destination_dir, count=True)
        elif api == 'swift':
            current_objects = bm.object_list_swift(s3, bucket_name, prefix=destination_dir, count=True)
        print()
        print(f'Done.\nFinished at {datetime.now()}, elapsed time = {datetime.now() - start}', flush=True)

        current_objects = pd.DataFrame.from_dict({'CURRENT_OBJECTS':current_objects})

        print(f'Current objects (with matching prefix): {len(current_objects)}', flush=True)

        if not current_objects.empty:
            print(f"Current objects (with matching prefix; excluding collated zips): {len(current_objects[current_objects['CURRENT_OBJECTS'].str.contains('collated_') == False])}", flush=True)
            print(f'Obtaining current object metadata, elapsed time = {datetime.now() - start}', flush=True)
            if api == 's3':
                current_objects['METADATA'] = current_objects['CURRENT_OBJECTS'].apply(find_metadata, bucket=bucket) # can't Daskify this without passing all bucket objects
            elif api == 'swift':
                current_objects = dd.from_pandas(current_objects, npartitions=len(client.scheduler_info()['workers'])*10)
                current_objects['METADATA'] = current_objects['CURRENT_OBJECTS'].apply(find_metadata_swift, conn=s3, container_name=bucket_name)
                current_objects = current_objects.compute()
            print(flush=True)
        else:
            current_objects['METADATA'] = None
        print(f'Done, elapsed time = {datetime.now() - start}', flush=True)

        ## check if log exists in the bucket, and download it and append top it if it does
        print(f'Checking for existing log files in bucket {bucket_name}, elapsed time = {datetime.now() - start}', flush=True)
        if current_objects['CURRENT_OBJECTS'].isin([log]).any():
            print(f'Log file {log} already exists in bucket. Downloading.')
            if api == 's3':
                bucket.download_file(log, log)
            elif api == 'swift':
                bm.download_file_swift(s3, bucket_name, log, log)
        elif current_objects['CURRENT_OBJECTS'].isin([previous_log]).any():
            print(f'Previous log file {previous_log} already exists in bucket. Downloading.')
            if api == 's3':
                bucket.download_file(previous_log, log)
            elif api == 'swift':
                bm.download_file_swift(s3, bucket_name, previous_log, log)
        print(f'Done, elapsed time = {datetime.now() - start}', flush=True)

        # boto3 does not allow for objects to be serialised
        if api == 's3':
            s3 = None
            del bucket

        # check local_dir formatting
        while local_dir[-1] == '/':
            local_dir = local_dir[:-1]

        zips_to_upload = True
        retries = 0
        global_retry_limit = 10
        while zips_to_upload and retries <= global_retry_limit:
            print(f'Processing files in {local_dir}, elapsed time = {datetime.now() - start}, try number: {retries+1}', flush=True)
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore')
                if api == 's3':
                    process_files(s3,
                                bucket_name,
                                api,
                                current_objects,
                                exclude,
                                local_dir,
                                destination_dir,
                                dryrun,
                                log,
                                global_collate,
                                use_compression,
                                client,
                                mem_per_worker,
                                collate_list_file,
                                save_collate_list,
                                file_count_stop
                            )
                elif api == 'swift':
                    process_files(s3,
                                bucket_name,
                                api,
                                current_objects,
                                exclude,
                                local_dir,
                                destination_dir,
                                dryrun,
                                log,
                                global_collate,
                                use_compression,
                                client,
                                mem_per_worker,
                                collate_list_file,
                                save_collate_list,
                                file_count_stop
                            )

            with open(collate_list_file, 'r') as clf:
                upload_checks = []
                for l in clf.readlines():
                    if l.split(',')[-1] == 'True':
                        upload_checks.append(True)
                    else:
                        upload_checks.append(False)
            print(upload_checks)
            zips_to_upload = any(upload_checks)
            retries += 1

    print(f'Finished uploads at {datetime.now()}, elapsed time = {datetime.now() - start}')
    print(f'Dask Client closed at {datetime.now()}, elapsed time = {datetime.now() - start}')
    print('Completing logging.')

    # Complete
    final_time = datetime.now() - start
    final_time_seconds = final_time.seconds + final_time.microseconds / 1e6
    try:
        logdf = pd.read_csv(log)
    except Exception as e:
        print(f'Error reading log file {log}: {e}')
        sys.exit()
    logdf = logdf.drop_duplicates(subset='DESTINATION_KEY', keep='last')
    logdf = logdf.reset_index(drop=True)
    logdf.to_csv(log, index=False)

    # Upload log file
    if not dryrun:
        print('Uploading log file.')
        upload_to_bucket(
            s3,
            bucket_name,
            api,
            local_dir,
            '/', #path
            log,
            os.path.basename(log),
            False, # dryrun
            mem_per_worker,
        )

    final_size = logdf["FILE_SIZE"].sum() / 1024**2
    try:
        final_transfer_speed = final_size / final_time_seconds

    except ZeroDivisionError:
        final_transfer_speed = 0
    try:
        final_transfer_speed_sperf = final_time_seconds / len(logdf)
    except ZeroDivisionError:
        final_transfer_speed_sperf = 0
    print(f'Finished at {datetime.now()}, elapsed time = {final_time}')
    print(f'Total: {len(logdf)} files; {(final_size):.2f} MiB; {(final_transfer_speed):.2f} MiB/s including setup time; {final_transfer_speed_sperf:.2f} s/file including setup time')
