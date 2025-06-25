import gc
import sys
import os
import warnings
from datetime import datetime
import numpy as np
import pandas as pd
from psutil import virtual_memory as mem
import bucket_manager.bucket_manager as bm
import swiftclient.exceptions
import swiftclient
import argparse
from dask import dataframe as dd
from distributed import Client
import subprocess
import logging
warnings.filterwarnings('ignore')


def logprint(msg: str, level: str = 'info'):  # , log: str | logging.Logger = None) -> None:
    """
    Logs a message to a specified log file or prints it to the console.

    Parameters:
    msg (str): The message to be logged or printed.

    log (str, optional): The file path to the log file. If None, the message
        is printed to the console. Defaults to None.

    Returns:
    None
    """
    # if isinstance(logger, bool):
    #     if logger:
    #         logger = None
    #     else:
    #         return None
    if isinstance(logger, logging.Logger):
        if level.lower() == 'info':
            logger.info(msg)
        elif level.lower() == 'debug':
            logger.debug(msg)
        elif level.lower() == 'warning':
            logger.warning(msg)
        elif level.lower() == 'error':
            logger.error(msg)
        elif level.lower() == 'critical':
            logger.critical(msg)
        else:
            logger.info(msg)
        return None
    # if logger is not None:
    #     with open(logger, 'a') as logfile:
    #         logfile.write(f'{msg}\n')
    else:
        print(msg, flush=True)
    return None


# @dask.delayed
def delete_object_swift(
    row: pd.Series,
    s3: swiftclient.Connection,
    bucket_name: str,
    del_metadata: bool = True,
    verify: bool = False,
    # log: str | logging.Logger = None
) -> bool:
    """
    Deletes an object and its associated metadata from a Swift storage bucket.

    Args:
        row (pd.Series): A pandas Series containing object information.
                         The 'CURRENT_OBJECTS' key should hold the name of the
                         object to delete.
        bucket_name (str): The name of the Swift bucket.
        s3 (swiftclient.Connection): A Swift client connection used to interact
            with the storage.
        del_metadata (bool, optional): Whether to delete the associated
            metadata object, i.e., .zip.metadata for .zip.
        log (str, optional): A log file path or identifier for logging
            messages. Defaults to None.

    Returns:
        bool: True if the primary object was successfully deleted, False
        otherwise.

    Logs:
        - Logs successful deletions of the object and its metadata.
        - Logs warnings if the metadata deletion fails.
        - Logs errors if the primary object deletion fails.
    """
    logprint(f"delete_object_swift called for {row['CURRENT_OBJECTS']}", 'debug')
    if verify:
        if row['verified'] is False:
            logprint(f'WARNING: {row["CURRENT_OBJECTS"]} not verified for deletion.', 'warning')
            return False
    obj = row['CURRENT_OBJECTS']
    if pd.isna(obj):
        return False
    deleted = False
    try:
        logprint(f"Attempting to delete {obj}", 'debug')
        s3.delete_object(bucket_name, obj)
        logprint(f'Deleted {obj}', 'info')
        deleted = True
    except Exception as e:
        logprint(f'Error deleting {obj}: {e}', 'info')
        return False
    if del_metadata:
        try:
            logprint(f"Attempting to delete metadata for {obj}", 'debug')
            s3.delete_object(bucket_name, f'{obj}.metadata')
            logprint(f'Deleted {obj}.metadata', 'info')
        except swiftclient.exceptions.ClientException as e:
            logprint(f'WARNING: Error deleting {obj}.metadata: {e.msg}', 'info')
    logprint(f"delete_object_swift completed for {row['CURRENT_OBJECTS']}, deleted={deleted}", 'debug')
    return deleted


# @dask.delayed
def is_orphaned_metadata(
    row: pd.Series,
    current_objects: pd.Series,
) -> bool:
    """
    Identifies orphaned metadata files in a Swift storage bucket.

    Args:
        row (pd.Series): A pandas Series containing object information.
            The 'CURRENT_OBJECTS' key should hold the name of the object to
            check.
        s3 (swiftclient.Connection): A Swift client connection used to interact
            with the storage.
        bucket_name (str): The name of the Swift bucket.
        current_objects (pd.Series): A pandas Series containing the list of
            current objects in the bucket.
        log (str): A log file path or identifier for logging messages.

    Returns:
        True if the object is a metadata object, has no parent zip object,
        and is successfully deleted.

    Notes:
        - The function checks if the metadata file exists and deletes it if
          it does not have a corresponding object in `current_objects`.
    """
    if row['CURRENT_OBJECTS'].endswith('.zip.metadata'):
        obj = row['CURRENT_OBJECTS']
    else:
        return False
    zip_obj = str(obj.split('.metadata')[0])
    in_co = len(current_objects[current_objects == zip_obj])
    return True if in_co == 0 else False


# @dask.delayed
def clean_orphaned_metadata(
    row: pd.Series,
    s3: swiftclient.Connection,
    bucket_name: str,
    # log: str | logging.Logger = None,
) -> bool:
    """
    Cleans up orphaned metadata files in a Swift storage bucket.

    Args:
        row (pd.Series): A pandas Series containing object information.
            The 'CURRENT_OBJECTS' key should hold the name of the object to
            check.
        s3 (swiftclient.Connection): A Swift client connection used to interact
            with the storage.
        bucket_name (str): The name of the Swift bucket.
        current_objects (pd.Series): A pandas Series containing the list of
            current objects in the bucket.
        log (str): A log file path or identifier for logging messages.

    Returns:
        True if the object is a metadata object, has no parent zip object,
        and is successfully deleted.

    Notes:
        - The function checks if the metadata file exists and deletes it if
          it does not have a corresponding object in `current_objects`.
    """
    if row['ORPHANED']:
        obj = row['CURRENT_OBJECTS']
        if not obj.endswith('.zip.metadata'):
            return False
        zip_obj = str(obj.split('.metadata')[0])
        try:
            delete_object_swift(row, s3, bucket_name, del_metadata=False, log=None)
            logprint(f'Deleted {obj} as {zip_obj} does not exist', log)
        except swiftclient.exceptions.ClientException as e:
            logprint(f'WARNING: Error deleting {obj}: {e.msg}', log)
        return True
    else:
        return False


# @dask.delayed
def verify_zip_objects(
    row: pd.Series,
    s3: swiftclient.Connection,
    bucket_name: str,
    remaining_objects_set: set,
    # remaining_objects_path: str,
    # logger_name: str = None,
) -> bool:
    """
    Verifies if the contents of a zip file stored in an S3 bucket are present
    in the current list of objects.

    Args:
        row (pd.Series): A pandas Series containing information about the zip
        object.
            The 'CURRENT_OBJECTS' key is expected to hold the zip file path.
        s3 (swiftclient.Connection): An active connection to the S3 storage.
        bucket_name (str): The name of the S3 bucket where the zip file is
            stored.
        current_objects_set (set): A set containing the current objects to
            verify against.
        log (str): A log file path or identifier for logging verification
            results.

    Returns:
        bool: True if all contents of the zip file are present in
        `current_objects`, False otherwise.

    Notes:
        - The function logs the verification result for each zip file.
        - The zip file's contents are prefixed with the parent directory path
          before comparison.
    """
    print(f"verify_zip_objects called for {row['CURRENT_OBJECTS']}"+ 'debug', flush=True)
    zip_obj = row['CURRENT_OBJECTS']
    # remaining_objects = pd.read_parquet(remaining_objects_path, engine='pyarrow')
    # remaining_objects_set = set(remaining_objects['CURRENT_OBJECTS'].tolist())
    # del remaining_objects  # Free memory

    if zip_obj == 'None':
        print(f'WARNING: {zip_obj} is None'+ 'warning', flush=True)
        return False
    path_stub = '/'.join(zip_obj.split('/')[:-1])
    zip_metadata_uri = f'{zip_obj}.metadata'

    try:
        print(f'Getting metadata from {zip_metadata_uri}' + ' debug', flush=True)
        zip_metadata = s3.get_object(bucket_name, zip_metadata_uri)[1]
    except swiftclient.exceptions.ClientException as e:
        print(f'WARNING: Error getting {zip_metadata_uri}: {e.msg}' + ' warning', flush=True)
        return False

    contents = [f'{path_stub}/{c}' for c in zip_metadata.decode().split('|') if c]
    lc = len(contents)
    verified = False
    # existing = []
    # try:
    #     with open(remaining_objects_path, 'r') as f:
    #         for c in contents:  # iteration over contents faster than over remaining_objects
    #             existing.append(c in f.read())
    # except FileNotFoundError:
    #     print(f'WARNING: {remaining_objects_path} not found. Cannot verify contents.' + ' warning', flush=True)
    #     return False
    # all_contents_exist = all(existing)
    all_contents_exist = [c in remaining_objects_set for c in contents]  # Use set membership testing
    # logprint(f'Contents: {lc}', log)
    try:
        print(f'Verifying {zip_obj} contents against remaining objects' + ' debug', flush=True)
        # if sum(current_objects.isin(contents).values) == lc:  # inefficient
        # Use set membership testing for increased efficiency
        if all_contents_exist:
            print(f'All {lc} contents of {zip_obj} found in remaining objects' + ' debug', flush=True)
            verified = True
            print(f'{zip_obj} verified: {verified} - can be deleted' + ' debug', flush=True)
        else:
            verified = False
            print(f'{zip_obj} verified: {verified} - cannot be deleted' + ' debug', flush=True)
    except Exception as e:
        print(f'Error verifying {zip_obj}: {e}' + ' error', flush=True)
        verified = False
    del zip_metadata, contents, existing  # Free memory
    print(f"verify_zip_objects completed for {row['CURRENT_OBJECTS']}, verified={verified}" + ' debug', flush=True)
    gc.collect()
    return verified


if __name__ == '__main__':
    epilog = ''

    class MyParser(argparse.ArgumentParser):
        def error(self, message):
            logger.error(f'Error parsing arguments: {message}')
            self.print_help()
            logger.debug('Exiting because of CLI parsing error.')
            sys.exit(2)
    parser = MyParser(
        description='Clean up collated_***.zip objects in S3 bucket, where *** = an integer.',
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '--api',
        type=str,
        help='API to use; "S3" or "Swift". Case insensitive. Note: S3 '
        'currently not implemented as only Swift is parallelisable through Dask.',
        default='Swift'
    )
    parser.add_argument(
        '--bucket-name',
        '-b',
        type=str,
        help='Name of the S3 bucket. Required.'
    )
    parser.add_argument(
        '--prefix',
        '-p',
        type=str,
        help='Prefix to be used in S3 object keys. Required.'
    )
    parser.add_argument(
        '--nprocs',
        '-n',
        type=int,
        help='Total number of CPU cores to use for parallel upload. Default is 4.',
        default=24
    )
    parser.add_argument(
        '--nthreads',
        '-t',
        type=int,
        help='Number of threads per worker to use for parallel upload. Default is 2.',
        default=2
    )
    parser.add_argument(
        '--clean-up-metadata',
        '-m',
        default=False,
        action='store_true',
        help='Clean up orphaned metadata files. Default is False.'
    )
    parser.add_argument(
        '--dryrun',
        '-d',
        default=False,
        action='store_true',
        help='Perform a dry run without uploading files or deleting zips. Default is False.'
    )
    parser.add_argument(
        '--yes',
        '-y',
        default=False,
        action='store_true',
        help='Answer yes to all prompts. Default is False.'
    )
    parser.add_argument(
        '--verify-skip',
        '-v',
        default=False,
        action='store_true',
        help='Skip verification the contents of the zip file are in the list of uploaded files *before* '
             'deletion. Default is False.'
    )
    parser.add_argument(
        '--debug',
        '-D',
        default=False,
        action='store_true',
        help='Enable debug logging. Default is False.'
    )
    args = parser.parse_args()

    # Set up logging
    debug = args.debug
    debug_level = logging.DEBUG if debug else logging.INFO

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger('clean_zips')
    logger.setLevel(debug_level)

    # Parse arguments
    api = args.api.lower()
    if api not in ['s3', 'swift']:
        logprint('API set to "swift".', 'info')
        api = 'swift'

    if not args.bucket_name:
        logger.error('Bucket name not provided.')
        parser.print_help()
        logger.debug('Exiting because bucket name not provided.')
        sys.exit(1)
    else:
        bucket_name = args.bucket_name
    if not args.prefix:
        prefix = None
    else:
        prefix = args.prefix

    nprocs = args.nprocs
    dryrun = args.dryrun
    yes = args.yes
    verify = not args.verify_skip  # Default is to verify
    clean_metadata = args.clean_up_metadata
    num_threads = args.nthreads
    if num_threads > nprocs:
        logprint(
            f'Number of threads ({num_threads}) cannot be greater than number of processes ({nprocs}). '
            f'Setting threads per worker to {nprocs // 2}.',
            'info'
        )
        logger.debug('Forcing number of threads per worker to nprocs // 2.')
        num_threads = nprocs // 2

    logprint(
        f'API: {api}, Bucket name: {bucket_name}, Prefix: {prefix}, nprocs: {nprocs}, dryrun: {dryrun}',
        'info'
    )

    # Print hostname
    uname = subprocess.run(['uname', '-n'], capture_output=True)
    logprint(f'Running on {uname.stdout.decode().strip()}', 'info')

    # Initiate timing
    start = datetime.now()

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
        logger.error(f'Environment AssertionError {e}')
        sys.exit(1)
    except KeyError as e:
        logger.error(f'Environment KeyError {e}')
        sys.exit(1)
    except ValueError as e:
        logger.error(f'Environment ValueError {e}')
        sys.exit(1)

    logprint(f'Using {api.capitalize()} API with host {s3_host}', 'info')

    if api == 's3':
        logger.error('Exiting because S3 API is not supported for parallelism with Dask. Please use Swift.')
        sys.exit('Currently only Swift is supported for parallelism with Dask. Exiting.')

    elif api == 'swift':
        s3 = bm.get_conn_swift()
        bucket_list = bm.bucket_list_swift(s3)

    if bucket_name not in bucket_list:
        logger.error(f'Bucket {bucket_name} not found in {api} bucket list. Exiting.')
        sys.exit(1)

    if api == 's3':
        bucket = s3.Bucket(bucket_name)
    elif api == 'swift':
        bucket = None

    success = False

    ############################
    #        Dask Setup        #
    ############################
    total_memory = mem().total
    n_workers = nprocs // num_threads  # e.g., 48 / 2 = 24
    mem_per_worker = mem().total // n_workers  # e.g., 187 GiB / 48 * 2 = 7.8 GiB

    logprint(
        f'nprocs: {nprocs}, Threads per worker: {num_threads}, Number of workers: {n_workers}, '
        f'Total memory: {total_memory/1024**3:.2f} GiB, Memory per worker: '
        f'{mem_per_worker/1024**3:.2f} GiB',
        'info'
    )

    # Process the files
    with Client(
        n_workers=n_workers,
        threads_per_worker=num_threads,
        memory_limit=f'{(int((mem().total/1024**3)*7/8)/n_workers)}GB'
    ) as client:
        logprint(f'Dask Client: {client}', 'info')
        logprint(f'Dashboard: {client.dashboard_link}', 'info')
        logprint(
            f'Starting processing at {datetime.now()}, elapsed time = {datetime.now() - start}',
            'info'
        )
        logprint(f'Using {nprocs} processes.', 'info')
        logprint(
            f'Getting current object list for {bucket_name}. This may take some time. Starting at '
            f'{datetime.now()}, elapsed time = {datetime.now() - start}',
            'info'
        )

        if api == 's3':
            current_object_names = bm.object_list(bucket, prefix=prefix, count=False)
        elif api == 'swift':
            current_object_names = bm.object_list_swift(s3, bucket_name, prefix=prefix, count=False)
        current_object_names = pd.DataFrame.from_dict({'CURRENT_OBJECTS': current_object_names})
        logprint(f'Done at {datetime.now()}, elapsed time = {datetime.now() - start}', 'info')

        # rands = np.random.randint(0, 9e6, size=2)
        # while rands[0] >= rands[1] or rands[0] < 0 or rands[1] >= len(current_object_names) or rands[1] == rands[0] or rands[1] - rands[0] > 1000000 or rands[1] - rands[0] < 100000:
        #     rands = np.random.randint(0, 9e6, size=2)
        # logprint(f'Taking a random slice for testing: {rands}', 'debug')
        # if debug:
        #     current_object_names = current_object_names[min(rands):max(rands)]

        current_objects = dd.from_pandas(
            current_object_names,
            npartitions=100
        )
        num_co = len(current_object_names)  # noqa
        del current_object_names  # Free memory
        gc.collect()  # Collect garbage to free memory

        logprint(f'Current_objects Partitions: {current_objects.npartitions}', 'info')
        logprint(f'Found {num_co} objects (with matching prefix) in bucket {bucket_name}.',
                 'info')
        if num_co > 0:
            zip_match = r".*collated_\d+\.zip$"
            metadata_match = r".*collated_\d+\.zip\.metadata$"
            current_objects['is_zip'] = current_objects['CURRENT_OBJECTS'].map_partitions(
                lambda partition: partition.str.fullmatch(zip_match, na=False)  # noqa
            )
            logprint('Reducing current_objects to only zip files.', 'info')
            current_zips = current_objects[current_objects['is_zip'] == True]  # noqa
            # client.scatter(current_objects)
            current_zips = client.persist(current_zips)  # Persist the Dask DataFrame
            num_cz = len(current_zips)  # noqa
            remaining_objects_set = current_objects[current_objects['is_zip'] == False]['CURRENT_OBJECTS']  # noqa
            remaining_objects_set = set(remaining_objects_set.compute())  # Convert to set for faster lookups
            # ro_path = 'remaining_objects.csv'
            # logprint(f'Saving remaining_objects (names only) to {ro_path}', 'info')
            # remaining_objects.to_csv(ro_path, index=False, single_file=True)  # Save remaining objects to CSV
            # logprint(f'Scattering remaining object names (non-zip files): {num_co - num_cz}', log=logger)
            # logprint(f'Size in memory of remaining_objects_set: {sys.getsizeof(remaining_objects_set) / 1024**2:.2f} MB', log=logger)
            client.scatter(remaining_objects_set, broadcast=True)  # Scatter remaining objects set for faster lookups

            del current_objects  # Free memory
            gc.collect()  # Collect garbage to free memory
            logprint(f'Persisted current_zips, len: {num_cz}', 'debug')
            logprint(f'Current_zips Partitions: {current_zips.npartitions}', 'debug')

            logprint(
                f'Found {num_cz} zip files (with matching prefix) in bucket {bucket_name}.',
                'info'
            )

            if num_cz > 0:
                logprint('Verifying zips can be deleted (i.e., contents exist).', 'info')
                logprint(f'npartitions: {current_zips.npartitions}', 'debug')
                if verify:
                    current_zips['verified'] = current_zips.map_partitions(
                        lambda partition: partition.apply(
                            verify_zip_objects,
                            axis=1,
                            args=(
                                s3,
                                bucket_name,
                                remaining_objects_set,
                                # ro_path,
                                # 'clean_zips',
                            ),
                        ),
                        meta=('bool'),
                    )
                if dryrun:
                    logprint(f'Current objects (with matching prefix): {num_co}', 'info')
                    if verify:
                        current_zips = client.persist(current_zips)
                        logprint(
                            f'{len(current_zips[current_zips["verified"] == True])} zip objects '
                            'were verified as deletable.',
                            'info'
                        )
                    else:
                        logprint(
                            f'Current zip objects (with matching prefix): {num_cz} '
                            'would be deleted.',
                            'info'
                        )
                    logger.debug('Dry run mode enabled. Exiting without deleting files.')
                    sys.exit()
                else:
                    logprint(f'Current objects (with matching prefix): {num_co}', 'info')
                    if not verify:
                        logprint(
                            f'Current zip objects (with matching prefix): {num_cz} will be '
                            'deleted.',
                            'info'
                        )
                    else:
                        logprint(
                            f'Current zip objects (with matching prefix): {num_cz} will be '
                            'deleted if all contents exist as objects.',
                            'info'
                        )
                    logprint('WARNING! Files are about to be deleted!', 'info')
                    logprint('Continue [y/n]?', 'info')
                    if not yes:
                        if input().lower() != 'y':
                            logprint('User did not confirm deletion. Exiting.', 'error')
                            logger.debug('Exiting because user did not confirm deletion.')
                            sys.exit(0)
                    else:
                        logprint('auto y', 'info')

                    logprint('Preparing to delete zip files.', 'info')
                    # current_zips = current_zips.persist()
                    current_zips['DELETED'] = current_zips.map_partitions(
                        lambda partition: partition.apply(
                            delete_object_swift,
                            axis=1,
                            args=(
                                s3,
                                bucket_name,
                                True,
                                verify,
                                # logger,
                            ),
                        ),
                        meta=('bool'),
                    )
                    logprint('Persisting current_zips.', 'debug')
                    # Persist and process current_zips in manageable chunks to avoid memory issues
                    # chunk_size = 10000  # Adjust as needed based on memory constraints
                    # num_chunks = (len(current_zips) // chunk_size) + 1

                    for i in range(current_zips.npartitions):
                        logprint(f'Processing partition {i}', 'debug')
                        part = current_zips.get_partition(i).compute()
                        del part
                        gc.collect()

                    if verify:
                        logprint(
                            f'{len(current_zips[current_zips["verified"] == True])} zip files were verified.',
                            'info'
                        )
                        logprint(
                            f'{len(current_zips[current_zips["DELETED"] == True])} zip files were DELETED.',
                            'info'
                        )
                        logprint(
                            f"{len(current_zips[current_zips['verified'] == False])} zip files were not "
                            "verified and not deleted.",
                            'info'
                        )
                        if len(
                            current_zips[current_zips['verified'] == True]  # noqa
                        ) != len(
                            current_zips[current_zips['DELETED'] == True]  # noqa
                        ):
                            logprint(
                                "Some errors may have occurred, as some zips verified for deletion were not "
                                "deleted.",
                                'warning'
                            )
                    else:
                        logprint(
                            f'{len(current_zips[current_zips["DELETED"] == True])} zip files were DELETED.',
                            'info'
                        )
                    del current_zips
                    logprint(
                        f'Finished processing at {datetime.now()}, elapsed time = {datetime.now() - start}',
                        'info'
                    )

            else:
                logprint(f'No zip files in bucket {bucket_name}.', 'warning')

            if clean_metadata:
                logprint('Checking for orphaned metadata files.', 'info')
                current_objects = dd.from_pandas(
                    pd.DataFrame.from_dict(
                        {'CURRENT_OBJECTS': bm.object_list_swift(s3, bucket_name, prefix=prefix, count=False)}
                    ),
                    npartitions=100
                )
                logprint(
                    f'Done.\nFinished at {datetime.now()}, elapsed time = {datetime.now() - start}',
                    'info'
                )

                current_objects['is_metadata'] = current_objects['CURRENT_OBJECTS'].map_partitions(
                    lambda partition: partition.str.fullmatch(metadata_match, na=False)
                )
                current_zip_names = client.persist(current_objects[current_objects['is_zip'] == True]['CURRENT_OBJECTS'])  # noqa
                md_objects = client.persist(current_objects[current_objects['is_metadata'] == True])  # noqa
                len_md = len(md_objects)
                if len_md > 0:  # noqa
                    md_objects = dd.from_pandas(md_objects, npartitions=100)  # noqa
                    if dryrun:
                        logprint(
                            'Any orphaned metadata files would be found and deleted.',
                            'info'
                        )
                    else:
                        md_objects['ORPHANED'] = md_objects.map_partitions(
                            lambda partition: partition.apply(
                                is_orphaned_metadata,
                                axis=1,
                                args=(
                                    current_zip_names,
                                )
                            ),
                            meta=('bool')
                        )
                        md_objects['deleted'] = md_objects.map_partitions(  # noqa
                            lambda partition: partition.apply(
                                clean_orphaned_metadata,
                                axis=1,
                                args=(
                                    s3,
                                    bucket_name,
                                    # logger,
                                )
                            ),
                            meta=('bool')
                        )
                        md_objects = client.persist(md_objects)
                        logprint(
                            f'{len(md_objects["deleted" == True])} '  # noqa
                            'orphaned metadata files were DELETED.',
                            'info'
                        )
                else:
                    logprint(
                        f'No metadata files in bucket {bucket_name}.',
                        'warning'
                    )
        else:
            logprint(f'No files in bucket {bucket_name}. Exiting.', 'warning')
            sys.exit(0)
