#!/usr/bin/env python
# coding: utf-8
# D.McKay Jun 2024

from datetime import datetime
import sys
import os
from multiprocessing import cpu_count
from distributed import Client
from dask_kubernetes.operator import KubeCluster
from dask import dataframe as dd
import gc
from psutil import virtual_memory as mem
import pyarrow as pa
import pandas as pd
from numpy.random import randint
import io
import zipfile
import warnings
import swiftclient.exceptions
import hashlib
import bucket_manager.bucket_manager as bm
import swiftclient
import argparse
import re
import shutil
import logging
from string import ascii_lowercase as letters
warnings.filterwarnings('ignore')


NAMESPACE_FILE = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
logger = logging.getLogger('process_collated_zips')


# Check if the namespace file exists and read it
def get_current_namespace():
    """Reads the namespace from the file system if available."""
    if os.path.exists(NAMESPACE_FILE):
        with open(NAMESPACE_FILE, 'r') as f:
            return f.read().strip()
    return "default"  # Fallback


def get_md5_hash(data: bytes) -> str:
    """
    Returns the MD5 hash of the given data.
    Args:
        data (bytes): The data to hash.
    Returns:
        str: The MD5 hash of the data.
    """
    return hashlib.md5(data).hexdigest()


def get_random_dir() -> str:
    """
    Generates a random file path for a CSV file.
    The function creates a random directory name using a random integer
    between 0 and 1,000,000 (inclusive) and returns a string representing
    the path to a CSV file within that directory.
    Returns:
        str: A string representing the random file path for a CSV file.
    """
    return f'/tmp/{randint(0,1e6):06d}'


def get_random_parquet_path() -> str:
    """
    Generates a random file path for a Parquet file.
    The function creates a random directory name using a random integer
    between 0 and 1,000,000 (inclusive) and returns a string representing
    the path to a Parquet file within that directory.
    Returns:
        str: A string representing the random file path for a Parquet file.
    """
    return f'/tmp/{randint(0,1e6):06d}/data.parquet'


def rm_parquet(path: str) -> None:
    """
    Remove the parent directory of the given path if it is a directory and not
    a symbolic link.
    If the parent is not a directory but exists, remove it as a file.
    Designed to work with temporary directories created by the
    get_random_parquet_path function.
    Args:
        path (str): The path to a file or directory whose parent directory is
        to be removed.
    Returns:
        None
    """
    parent = os.path.dirname(path)
    if os.path.isdir(parent) and not os.path.islink(parent):
        shutil.rmtree(parent)
    elif os.path.exists(parent):
        os.remove(parent)


def find_metadata_swift(row: pd.Series, conn: swiftclient.Connection, bucket_name: str) -> str:
    """
    Retrieve metadata for a given key from a Swift container.
    This function attempts to retrieve metadata for a specified key from a
    Swift container.
    It handles both '.zip' files and other types of files differently. If the
    key ends with '.zip', it tries to fetch the metadata either by getting the
    object or by checking the object's headers. The metadata is expected to be
    a string separated by '|'.

    Args:
        row (pd.Series): Row of dataframe with a 'key' column which contains
        the object key.

        conn: The connection object to the Swift service.

        container_name (str): The name of the container in the Swift service.

    Returns:
        str: '|'-separated metadata strings if found, otherwise None.
    """
    if not row['is_zipfile']:
        return ''
    key = row['key']
    if isinstance(key, str):
        existing_zip_contents = None
        try:
            existing_zip_contents = str(
                conn.get_object(bucket_name, ''.join([key, '.metadata']))[1].decode('UTF-8')
            )  # .split('|') # use | as separator
            logger.info(f'Using zip-contents-object, {"".join([key, ".metadata"])} for object {key}.')
        except Exception:
            try:
                existing_zip_contents = conn.head_object(
                    bucket_name,
                    key
                )['x-object-meta-zip-contents']  # .split('|') # use | as separator
                logger.info(f'Using zip-contents metadata for {key}.')
            except KeyError:
                return ''
            except Exception:
                return ''
        if existing_zip_contents is not None:
            if '|' not in existing_zip_contents:
                existing_zip_contents = '|'.join(
                    existing_zip_contents.split(',')
                )  # some older metadata may have used commas as separators
            return existing_zip_contents
        else:
            return ''
    else:
        return ''


def object_list_swift(
    conn: swiftclient.Connection,
    container_name: str,
    prefix: str = '',
    full_listing: bool = True,
    count: bool = False
) -> list[str]:
    """List objects in a Swift container.

        Args:
            conn: Swift connection object.

            container_name: Name of the container.

            prefix: Only list objects starting with this prefix.
            Defaults to ''.

            full_listing:  Whether to get a full listing. Defaults to True.

            count: Whether to print a running count of objects listed.
            Defaults to False.

        Returns:
            A list of object names in the container.
        """
    keys = []
    if count:
        o = 0
    for obj in conn.get_container(container_name, prefix=prefix, full_listing=full_listing)[1]:
        keys.append(obj['name'])
        if count:
            o += 1
            if o % 10000 == 0:
                logger.info(f'Existing objects: {o}', end='\r')
    print()
    return keys


def match_key(row: pd.Series) -> bool:
    """
    Determines if the 'key' value in a given pandas Series matches a specific
        pattern.

    The function checks if the 'key' field in the input row matches the pattern
    `.*collated_\\d+\\.zip$`, which corresponds to strings ending with
    "collated_" followed by one or more digits and the ".zip" extension.

    Args:
        row (pd.Series): A pandas Series object containing a 'key' field to be
        checked.

    Returns:
        bool: True if the 'key' matches the specified pattern, False otherwise.
    """
    key = row['key']
    pattern = re.compile(r'.*collated_\d+\.zip$')
    if pattern.match(key):
        # logger.debug(key)
        return True
    else:
        return False


def prepend_zipfile_path_to_contents(row: pd.Series) -> str:
    """
    Prepends the path of the zip file to the contents of the given row.
    This function takes a pandas Series object representing a row, extracts
    the path from the 'key' column, and prepends it to the 'contents' column.
    The modified 'contents' value is then returned.
    Args:
        row (pd.Series): A pandas Series object containing at least 'key' and
        'contents' columns.

    Returns:
        str: The modified 'contents' value with the prepended path.
    """
    if not row['is_zipfile']:
        return ''
    path_stub = '/'.join(row['key'].split('/')[:-1])
    contents = row['contents'].split('|')
    prepended = [f'{path_stub}/{c}' for c in contents]
    return '|'.join(prepended)


def explode_zip_contents(df: pd.DataFrame) -> pd.DataFrame:
    """
    For a pandas DataFrame of zip files with a 'contents' column,
    returns a new DataFrame mapping each zip to its individual contents.
    """
    output_rows = []
    for row in df.itertuples():
        # Skip non-zip files or rows where contents couldn't be read
        if not row.is_zipfile or not isinstance(row.contents, str) or not row.contents:
            continue

        zip_filename = row.key
        contents = row.contents.split('|')
        total_contents = len(contents)

        for content_file in contents:
            output_rows.append({
                'zip_filename': zip_filename,
                'content_filename': content_file,
                'total_contents': total_contents
            })

    if not output_rows:
        return pd.DataFrame(columns=['zip_filename', 'content_filename', 'total_contents'])
    return pd.DataFrame(output_rows)


def extract_and_upload(row: pd.Series, conn: swiftclient.Connection, bucket_name: str) -> bool:
    """
    Extracts the contents of a zip file from an object storage bucket and
    uploads the extracted files back to the bucket if not already present.
    Files will be skipped if they are already present and have the same MD5
    hash, or if they are segmented.
    Args:
        row (pd.Series): row of a dataframe with a 'key' column that gives the
        path of the zip file in the object storage bucket.

        conn (swiftclient.Connection): The connection object to interact with
        the object storage.

        bucket_name (str): The name of the bucket where the zip file is stored
        and where the extracted files will be uploaded.

    Returns:
        bool: True if the extraction and upload process is completed
        successfully, False otherwise.

    Raises:
        swiftclient.exceptions.ClientException: If there is an error
        interacting with the object storage.
    """
    if not row['extract']:
        return False
    key = row['key']
    done = False
    start = datetime.now()
    size = 0
    uploaded = False
    # logger.debug(f'Extracting {row["key"]}...')
    path_stub = '/'.join(key.split('/')[:-1])
    try:
        zipfile_data = io.BytesIO(conn.get_object(bucket_name, key)[1])
    except swiftclient.exceptions.ClientException as e:
        if e.http_status == 404:
            logger.info(f'Object {key} not found - possibly already deleted. '
                   'Skipping and marking as True to allow clean up.')
            return True
        else:
            raise
    with zipfile.ZipFile(zipfile_data) as zf:
        num_files = len(zf.namelist())
        for content_file in zf.namelist():
            # logger.debug(content_file)
            content_file_data = zf.open(content_file)
            size += len(content_file_data.read())
            content_file_data.seek(0)
            content_key = path_stub + '/' + content_file
            content_md5 = get_md5_hash(content_file_data.read())
            content_file_data.seek(0)
            # Previous check for existing content consider whether _all_ of
            # the files in a zip have been uploaded.
            # Here we check individual files and only upload if the md5s
            # differ.
            try:
                existing_content = conn.head_object(bucket_name, content_key)
                existing_md5 = existing_content['etag']
                if existing_md5 == content_md5:
                    # logger.debug(f'Skipping {content_key} ({existing_md5} == {content_md5})')
                    continue
                elif existing_md5 != content_md5 and '-' in existing_md5:
                    logger.info(
                        f'Skipping {content_key} (exists, but segmented - replacement not '
                        'currently supported).'
                    )
                    continue
                else:
                    logger.info(f'Uploading {content_key} ({existing_md5} != {content_md5}).')
                    # logger.debug('Content differs. Uploading.')
                    conn.put_object(bucket_name, content_key, content_file_data)
                    uploaded = True
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    # logger.debug('Object not found. Uploading.')
                    # logger.debug(f'Uploading {content_key} (not found).')
                    conn.put_object(bucket_name, content_key, content_file_data)
                    uploaded = True
                else:
                    raise
            del content_file_data
            # logger.debug(f'Uploaded {content_file} to {key}')
    done = True
    del zipfile_data
    gc.collect()
    end = datetime.now()
    duration = (end - start).total_seconds()
    try:
        if uploaded:
            logger.info(
                f'Extracted and uploaded contents of {key} ({num_files} files, '
                f'total size: {size/1024**2:.2f} MiB) in {duration:.2f} s '
                f'({(size/1024**2/duration):.2f} MiB/s if all files were uploaded).'
            )
        else:
            logger.info(f'Extracted contents of {key} ({num_files} files, '
                   f'total size: {size/1024**2:.2f} MiB) in {duration:.2f} s '
                   '(no uploads required)')
    except ZeroDivisionError:
        if uploaded:
            logger.info(f'Extracted and uploaded contents of {key} ({num_files} files, '
                   f'total size: {size/1024**2:.2f} MiB) in {duration:.2f} s')
        else:
            logger.info(f'Extracted contents of {key} ({num_files} files, '
                   f'total size: {size/1024**2:.2f} MiB) in {duration:.2f} s '
                   '(no uploads required)')

    return done


def main():
    """
    Main function to process collated zip files in an S3 bucket on
    echo.stfc.ac.uk.

    This script performs the following tasks:
        1. Parses command-line arguments to determine the S3 bucket name,
           whether to list zip files, whether to extract and upload zip
           files, and the maximum number of processes to use.

        2. Validates the provided arguments and sets up the necessary
           configurations.

        3. Connects to the specified S3 bucket and retrieves the list of
        objects.

        4. Uses Dask to parallelize the processing of the objects in the
        bucket.

        5. Identifies zip files and optionally lists them.

        6. Extracts and uploads the contents of the zip files if specified.

    Command-line arguments:
         --bucket-name, -b: Name of the S3 bucket (required).

         --list-zips, -l: List the zip files in the bucket.

         --extract, -e: Extract and upload zip files whose contents are not
                        found in the bucket.

         --nprocs, -n: Maximum number of processes to use for extraction and
                       upload (default: 6).
    Raises:
         SystemExit: If invalid arguments are provided or if the bucket is not
         found.
    """

    all_start = datetime.now()
    logger.info(f'Start time: {all_start}.')
    epilog = ''

    class MyParser(argparse.ArgumentParser):
        def error(self, message):
            sys.stderr.write(f'error: {message}\n\n')
            self.print_help()
            sys.exit(2)
    parser = MyParser(
        description='Search for zip files in a given S3 bucket on echo.stfc.ac.uk.',
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--bucket-name',
        '-b',
        type=str,
        help='Name of the S3 bucket.',
        required=True
    )
    parser.add_argument(
        '--list-zips',
        '-l',
        action='store_true',
        help='List the zip files.'
    )
    parser.add_argument(
        '--extract',
        '-e',
        action='store_true',
        help='Extract and upload zip files for which the contents are not found in the bucket.'
    )
    parser.add_argument(
        '--dask-workers',
        '-w',
        type=int,
        default=4,
        help='Number of Dask workers to use. Default is 4.'
    )
    parser.add_argument(
        '--nthreads',
        '-t',
        type=int,
        help='Number of threads per worker to use for parallel upload. Default is 4.',
        default=4
    )
    parser.add_argument(
        '--prefix',
        '-p',
        type=str,
        help='Prefix to use for the S3 bucket. Defaults to empty string.',
        default=''
    )
    parser.add_argument(
        '--recover',
        '-r',
        action='store_true',
        help='Recover from a previous run. Will only work if data.parquet is copied to the working directory.'
    )

    args = parser.parse_args()

    # Set up logging
    debug = args.debug
    debug_level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(debug_level)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    bucket_name = args.bucket_name
    if args.list_zips:
        list_zips = True
    else:
        list_zips = False

    if args.extract:
        extract = True
    else:
        extract = False

    if args.recover:
        if not list_zips:
            recover = True
        else:
            logger.error('Cannot list zips and recover at the same time. Exiting.')
            sys.exit()
    else:
        recover = False

    if args.prefix:
        prefix = args.prefix
    else:
        prefix = ''

    if list_zips and extract:
        logger.error('Cannot list contents and extract at the same time. Exiting.')
        sys.exit()

    # Setup bucket object
    try:
        assert bm.check_keys(api='swift')
    except AssertionError as e:
        print(e)
        sys.exit()

    conn = bm.get_conn_swift()
    bucket_list = bm.bucket_list_swift(conn)

    if bucket_name not in bucket_list:
        print(f'Bucket {bucket_name} not found in {os.environ["ST_AUTH"]}.')
        sys.exit()

    print(f'Using bucket {bucket_name}.')
    if not recover:
        print('Getting key list...')
        keys = pd.DataFrame.from_dict({'key': object_list_swift(conn, bucket_name, prefix, count=True)})
        print(keys.head())

    ############################
    #        Dask Setup        #
    ############################
    dask_workers = args.dask_workers
    num_threads = args.nthreads
    # large number as each worker may need to extract multiple files
    total_memory = mem().total

    mem_limit = int(total_memory * 0.8) // dask_workers  # Limit memory to 0.8 total memory
    mem_request = int(total_memory * 0.5) // dask_workers  # Request memory to 0.5 total memory
    cpus_per_worker = num_threads  # Number of CPUs per worker
    # Leave some CPUs for the scheduler and other processes
    max_cpus_per_worker = (cpu_count() - 8) / dask_workers

    # K8s pod info
    namespace = get_current_namespace()
    if namespace == 'default':
        namespace = 'dask-service-clusters'

    tag = ''
    for i in range(6):
        tag += letters[randint(0, 25)]

    logger.info(f'Using namespace: {namespace}')
    cluster = KubeCluster(
        name="dask-cluster-cleanzips-" + tag,
        image="ghcr.io/lsst-uk/ces:latest",
        namespace=namespace,
        n_workers=dask_workers,
        resources={
            "requests": {
                "memory": '64Gi',  # f'{mem_request}Gi',
                "cpu": '8'  # f'{cpus_per_worker}'
            },
            "limits": {
                "memory": '96Gi',  # f'{mem_limit}Gi',
                "cpu": '16'  # f'{max_cpus_per_worker}'
            }
        },
    )

    with Client(cluster) as client:
        logger.info(f'Dask Client: {client}')
        logger.info(f'Dashboard: {client.dashboard_link}')
        logger.info(f'Using {dask_workers} workers, each with {num_threads} threads.')

        if not recover:
            # Dask Dataframe of all keys
            # high chunksize allows enough mem for parquet to be written
            keys_df = dd.from_pandas(keys, chunksize=1000000)
            if extract:
                keys_only_df = keys_df['key'].copy()
                keys_only_df = client.persist(keys_only_df)
            del keys
            logger.info(f'Partitions: {keys_df.npartitions}')
            # logger.debug(keys_df)
            # Discover if key is a zipfile
            keys_df['is_zipfile'] = keys_df.map_partitions(
                lambda partition: partition.apply(
                    match_key,
                    axis=1,
                ),
                meta=('is_zipfile', 'bool')
            )

        # check, compute and write to parquet

        if list_zips:
            check = keys_df['is_zipfile'].any().compute()
            if not check:
                logger.warning('No zipfiles found. Exiting.')
                sys.exit()
            logger.info('Zip files found:')
            logger.info(keys_df[keys_df['is_zipfile'] == True]['key'])  # noqa
            logger.info(f'Done. Runtime: {datetime.now() - all_start}.')
            sys.exit()

        if extract:
            if not recover:
                # keys_df = client.persist(keys_df)
                check = keys_df['is_zipfile'].any().compute()
                if not check:
                    logger.warning('No zipfiles found. Exiting.')
                    sys.exit()
                # Get metadata for zipfiles
                keys_df['contents'] = keys_df.map_partitions(  # noqa
                    lambda partition: partition.apply(
                        find_metadata_swift,
                        axis=1,
                        args=(
                            conn,
                            bucket_name,
                        ),
                    ),
                    meta=('contents', 'str')
                )

                # Prepend zipfile path to contents
                # logger.debug(keys_df)
                keys_df['contents'] = keys_df.map_partitions(
                    lambda partition: partition.apply(
                        prepend_zipfile_path_to_contents,
                        axis=1
                    ),
                    meta=('contents', 'str'),
                )

                # --- Start of new verification logic ---
                logger.info('Verifying which zip files need extraction...')

                # 1. Create a clean DataFrame of all existing object keys for the merge
                all_keys_df = keys_df[['key']].rename(columns={'key': 'CURRENT_OBJECTS'})

                # 2. Explode all zip contents into a new Dask DataFrame
                zip_files_df = keys_df[keys_df['is_zipfile'] == True].persist()  # noqa
                exploded_contents = zip_files_df.map_partitions(
                    explode_zip_contents,
                    meta={
                        'zip_filename': 'object',
                        'content_filename': 'object',
                        'total_contents': 'int64'
                    }
                )

                # 3. Find which content files already exist with an efficient inner merge
                existing_contents = dd.merge(
                    exploded_contents,
                    all_keys_df,
                    left_on='content_filename',
                    right_on='CURRENT_OBJECTS',
                    how='inner'
                )

                # 4. Count total vs. existing contents to find incomplete zips
                # Count how many contents were found for each zip
                existing_counts = existing_contents.groupby('zip_filename').content_filename.count().persist()
                # Get the original total number of contents for each zip
                total_counts = exploded_contents.groupby('zip_filename').total_contents.first().persist()

                # Align the series, filling in 0 for zips where no contents were found
                existing_counts = existing_counts.reindex(total_counts.index.compute(), fill_value=0)

                # A zip needs to be extracted if the number of existing files is NOT equal to the total
                to_extract_series = (existing_counts.compute() != total_counts.compute())
                to_extract_df = to_extract_series[to_extract_series].reset_index()
                to_extract_df.columns = ['key', 'extract']

                # 5. Merge the 'extract' boolean back into the main DataFrame
                keys_df = dd.merge(
                    keys_df,
                    to_extract_df,
                    on='key',
                    how='left'
                ).fillna({'extract': False})  # Mark zips that don't need extraction as False

                # --- End of new verification logic ---

                # all wrangling and decision making done - write to parquet
                # for lazy unzipping
                # only require key and extract boolean
                pq = get_random_parquet_path()
                logger.info(f'tmp folder is {pq}')
                keys_df = keys_df[['key', 'extract']]
                keys_df.to_parquet(pq, schema=pa.schema([
                    ('key', pa.string()),
                    ('extract', pa.bool_()),
                ]))
                logger.info(f'Parquet file written to {os.path.abspath(pq)}.')

            else:
                pq = 'data.parquet'
                logger.info(f'Recovering from previous run. Using {os.path.abspath(pq)}.')

            logger.info('Extracting zip files...')
            keys_df = dd.read_parquet(
                pq,
                dtype={'key': 'str', 'extract': 'bool'},
                chunksize=100000
            )  # small chunks to avoid memory issues

            logger.info(f'Partitions: {keys_df.npartitions}')

            logger.info('Zip files extracted and uploaded:')
            keys_df['extracted_and_uploaded'] = keys_df.map_partitions(  # noqa
                lambda partition: partition.apply(
                    extract_and_upload,
                    axis=1,
                    args=(
                        conn,
                        bucket_name,
                    ),
                ),
                meta=('extracted_and_uploaded', 'bool')
            )
            # with annotate(resources={'MEMORY': 10e9}):
            extracted_and_uploaded = keys_df[
                keys_df['extract'] == True  # noqa
            ]['extracted_and_uploaded'].persist()
            del keys_df
            gc.collect()
            if extracted_and_uploaded.all().compute():
                logger.info('All zip files extracted and uploaded.')
                rm_parquet(pq)

    logger.info(f'Done. Runtime: {datetime.now() - all_start}.')


if __name__ == '__main__':
    main()
