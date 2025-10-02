#!/usr/bin/env python
# coding: utf-8
# D.McKay Jun 2024

from datetime import datetime
import sys
import os

from distributed import Client

from dask import dataframe as dd

from psutil import virtual_memory as mem

import pandas as pd

import io
import zipfile
import warnings
import swiftclient.exceptions

import bucket_manager.bucket_manager as bm
import swiftclient
import argparse

import shutil
import logging

warnings.filterwarnings('ignore')

logger = logging.getLogger('download_bucket')


def download_and_extract(row: pd.Series, conn: swiftclient.Connection, bucket_name: str) -> bool:
    if not row['download']:
        return False
    key = row['key']

    is_zip = False
    if key.lower().endswith('.zip'):
        is_zip = True
    path_stub = '/'.join(key.split('/')[:-1])
    if is_zip:
        logger.info(f'Downloading and extracting {key}...')
        try:
            zipfile_data = io.BytesIO(conn.get_object(bucket_name, key)[1])
        except swiftclient.exceptions.ClientException as e:
            if e.http_status == 404:
                logger.info(
                    f'Object {key} not found.'
                )
                return False
            else:
                raise
        with zipfile.ZipFile(zipfile_data) as zf:
            for content_file in zf.namelist():
                logger.info(f'Extracting {content_file}...')
                content_file_data = zf.open(content_file)
                with open('./' + os.path.join(path_stub, content_file), 'wb') as f:
                    shutil.copyfileobj(content_file_data, f)
        return True
    else:
        logger.info(f'Downloading {key}...')
        try:
            object_data = conn.get_object(bucket_name, key)[1]
            with open('./' + key, 'wb') as f:
                f.write(object_data)
        except swiftclient.exceptions.ClientException as e:
            if e.http_status == 404:
                logger.info(
                    f'Object {key} not found.'
                )
                return False
            else:
                raise
        return True


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


def main():

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
        '--prefix',
        '-p',
        type=str,
        help='Prefix to use for the S3 bucket. Defaults to empty string.',
        default=''
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

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    bucket_name = args.bucket_name

    if args.prefix:
        prefix = args.prefix
    else:
        prefix = ''

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

    with Client(n_workers=dask_workers, threads_per_worker=num_threads, memory_limit=mem_limit) as client:
        logger.info(f'Dask Client: {client}')
        logger.info(f'Dashboard: {client.dashboard_link}')
        logger.info(f'Using {dask_workers} workers, each with {num_threads} threads.')

        # Dask Dataframe of all keys
        # high chunksize allows enough mem for parquet to be written
        keys_df = dd.from_pandas(keys, chunksize=10000)
        keys_df['download'] = keys_df['key'].map_partitions(
            lambda series: series.apply(
                lambda key: not os.path.exists('./' + key.strip())
            ),
            meta=pd.Series(dtype=bool)
        )
        del keys
        logger.info(f'Partitions: {keys_df.npartitions}')

        # Download and extract
        keys_df['downloaded'] = keys_df.map_partitions(
            lambda partition: partition.apply(
                download_and_extract,
                axis=1,
                conn=conn,
                bucket_name=bucket_name
            ),
            meta=pd.Series(dtype=bool)
        )

    result = keys_df.compute()
    if result['downloaded'].all():
        logger.info('All files downloaded successfully.')
    else:
        logger.warning('Some files failed to download.')

    logger.info(f'Done. Runtime: {datetime.now() - all_start}.')


if __name__ == '__main__':
    main()
