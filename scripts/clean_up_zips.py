import sys
import os
import warnings
from datetime import datetime
import pandas as pd
import io
import zipfile
from psutil import virtual_memory as mem
import bucket_manager.bucket_manager as bm
import swiftclient.exceptions
import swiftclient
import argparse
from dask import dataframe as dd
from distributed import Client
import subprocess
warnings.filterwarnings('ignore')


def logprint(msg: str, log: str = None) -> None:
    """
    Logs a message to a specified log file or prints it to the console.

    Parameters:
    msg (str): The message to be logged or printed.

    log (str, optional): The file path to the log file. If None, the message
        is printed to the console. Defaults to None.

    Returns:
    None
    """
    if log is not None:
        with open(log, 'a') as logfile:
            logfile.write(f'{msg}\n')
    else:
        print(msg, flush=True)


def delete_object_swift(
    row: pd.Series,
    s3: swiftclient.Connection,
    bucket_name: str,
    del_metadata: bool = True,
    log: str = None
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
        - Prints errors to stderr if the primary object deletion fails.
    """
    obj = row['CURRENT_OBJECTS']
    deleted = False
    try:
        s3.delete_object(bucket_name, obj)
        logprint(f'Deleted {obj}', log)
        deleted = True
    except Exception as e:
        print(f'Error deleting {obj}: {e}', file=sys.stderr)
        return False
    if del_metadata:
        try:
            s3.delete_object(bucket_name, f'{obj}.metadata')
            logprint(f'Deleted {obj}.metadata', log)
        except swiftclient.exceptions.ClientException as e:
            logprint(f'WARNING: Error deleting {obj}.metadata: {e.msg}', log)
    return deleted


def clean_orphaned_metadata(
    row: pd.Series,
    s3: swiftclient.Connection,
    bucket_name: str,
    current_objects: pd.Series,
    log: str
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
    if row['CURRENT_OBJECTS'].endswith('.zip.metadata'):
        obj = row['CURRENT_OBJECTS']
    else:
        return False
    zip_obj = str(obj.split('.metadata')[0])
    in_co = len(current_objects[current_objects['CURRENT_OBJECTS'] == zip_obj])
    if in_co == 0:
        # The zip file does not exist, so delete the metadata
        try:
            delete_object_swift(row, s3, bucket_name, del_metadata=False, log=log)
            logprint(f'Deleted {obj} as {zip_obj} does not exist', log)
        except swiftclient.exceptions.ClientException as e:
            logprint(f'WARNING: Error deleting {obj}: {e.msg}', log)
    return True


def verify_zip_objects(
    row: pd.Series,
    s3: swiftclient.Connection,
    bucket_name: str,
    current_objects: pd.Series,
    log: str
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
        current_objects (pd.Series): A pandas Series containing the list of
            current objects to verify against.
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
    zip_obj = row['CURRENT_OBJECTS']
    zip_data = io.BytesIO(s3.get_object(bucket_name, zip_obj)[1])
    with zipfile.ZipFile(zip_data, 'r') as z:
        contents = z.namelist()
    path_stub = '/'.join(zip_obj.split('/')[:-1])
    contents = [f'{path_stub}/{c}' for c in contents]
    verified = False
    if set(contents).issubset(current_objects):
        verified = True
        logprint(f'{zip_obj} verified: {verified} - can be deleted', log)
    else:
        verified = False
        logprint(f'{zip_obj} verified: {verified} - cannot be deleted', log)
    del zip_data, contents
    # gc.collect()
    return verified


if __name__ == '__main__':
    epilog = ''

    class MyParser(argparse.ArgumentParser):
        def error(self, message):
            sys.stderr.write(f'error: {message}\n\n')
            self.print_help()
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
        help='Number of CPU cores to use for parallel upload. Default is 4.',
        default=4
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
        '--log-to-file',
        default=False,
        action='store_true',
        help='Log output to file. Default is False, i.e., stdout.'
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
    args = parser.parse_args()

    # Parse arguments
    api = args.api.lower()
    if api not in ['s3', 'swift']:
        print('API set to "swift".')
        api = 'swift'

    if not args.bucket_name:
        print('Bucket name not provided.', file=sys.stderr)
        parser.print_help()
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
    log_to_file = args.log_to_file
    verify = not args.verify_skip  # Default is to verify
    clean_metadata = args.clean_up_metadata

    print(f'API: {api}, Bucket name: {bucket_name}, Prefix: {prefix}, nprocs: {nprocs}, dryrun: {dryrun}')

    # Set up logging
    if log_to_file:
        log = f'clean_zips_{bucket_name}_{prefix}_{datetime.now().strftime("%Y%m%d%H%M%S")}.log'
        logprint(f'clean_zips_{bucket_name}_{prefix}_{datetime.now().strftime("%Y%m%d%H%M%S")}.log', log)
        if not os.path.exists(log):
            logprint(f'Created log file {log}', log)
    else:
        log = None
        logprint('Logging to stdout.', log)

    # Print hostname
    uname = subprocess.run(['uname', '-n'], capture_output=True)
    logprint(f'Running on {uname.stdout.decode().strip()}', log)

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
        print(f'AssertionError {e}', file=sys.stderr)
        sys.exit()
    except KeyError as e:
        print(f'KeyError {e}', file=sys.stderr)
        sys.exit()
    except ValueError as e:
        print(f'ValueError {e}', file=sys.stderr)
        sys.exit()

    logprint(f'Using {api.capitalize()} API with host {s3_host}')

    if api == 's3':
        print('Currently only Swift is supported for parallelism with Dask. Exiting.', file=sys.stderr)
        sys.exit()

    elif api == 'swift':
        s3 = bm.get_conn_swift()
        bucket_list = bm.bucket_list_swift(s3)

    if bucket_name not in bucket_list:
        print(f'Bucket {bucket_name} not found in {api} bucket list. Exiting.', file=sys.stderr)
        sys.exit()

    if api == 's3':
        bucket = s3.Bucket(bucket_name)
    elif api == 'swift':
        bucket = None

    success = False

    ############################
    #        Dask Setup        #
    ############################
    total_memory = mem().total
    n_workers = nprocs
    mem_per_worker = mem().total // n_workers  # e.g., 187 GiB / 48 * 2 = 7.8 GiB
    logprint(f'nprocs: {nprocs}, Threads per worker: 1, Number of workers: {n_workers}, '
             f'Total memory: {total_memory/1024**3:.2f} GiB, Memory per worker: '
             f'{mem_per_worker/1024**3:.2f} GiB')

    # Process the files
    with Client(n_workers=n_workers, threads_per_worker=1, memory_limit=mem_per_worker) as client:
        logprint(f'Dask Client: {client}', log=log)
        logprint(f'Dashboard: {client.dashboard_link}', log=log)
        logprint(f'Starting processing at {datetime.now()}, elapsed time = {datetime.now() - start}', log=log)
        logprint(f'Using {nprocs} processes.', log=log)
        logprint(f'Getting current object list for {bucket_name}. This may take some time.\nStarting at '
                 f'{datetime.now()}, elapsed time = {datetime.now() - start}', log=log)

        if api == 's3':
            current_objects = bm.object_list(bucket, prefix=prefix, count=False)
        elif api == 'swift':
            current_objects = bm.object_list_swift(s3, bucket_name, prefix=prefix, count=False)
        logprint(f'Done.\nFinished at {datetime.now()}, elapsed time = {datetime.now() - start}', log=log)
        len_co = len(current_objects)
        current_objects = dd.from_pandas(
            pd.DataFrame.from_dict({'CURRENT_OBJECTS': current_objects}),
            chunksize=10000
        )
        nparts = current_objects.npartitions

        logprint(f'Found {len(current_objects)} objects (with matching prefix) in bucket {bucket_name}.',
                 log=log)
        if len_co > 0:
            current_zips = current_objects[
                current_objects[
                    'CURRENT_OBJECTS'
                ].str.contains(
                    'collated_\d+\.zip'  # noqa
                ) & ~current_objects[
                    'CURRENT_OBJECTS'
                ].str.contains(
                    '.zip.metadata'
                )
            ].repartition(
                npartitions=int(
                    max(
                        nparts // nparts % n_workers, n_workers, nparts // n_workers
                    ) // n_workers * n_workers
                )
            )
            current_objects = current_objects.persist()
            client.scatter(current_objects, broadcast=True)
            print(f'n_partitions: {current_zips.npartitions}')
            len_cz = len(current_zips)
            len_md = len(
                current_objects[
                    current_objects['CURRENT_OBJECTS'].str.endswith('.zip.metadata')
                ]
            )
            logprint(
                f'Found {len_cz} zip files (with matching prefix) in bucket {bucket_name}.',
                log=log
            )

            if len_cz > 0:
                if verify:
                    current_zips['verified'] = current_zips.map_partitions(
                        lambda partition: partition.apply(
                            verify_zip_objects,
                            axis=1,
                            args=(
                                s3,
                                bucket_name,
                                current_objects['CURRENT_OBJECTS'],
                                log
                            ),
                        ),
                        meta=('bool')
                    )
                if dryrun:
                    logprint(f'Current objects (with matching prefix): {len_co}', log=log)
                    if verify:
                        current_zips = current_zips.compute()
                        logprint(
                            f'{len(current_zips[current_zips["verified"] == True])} zip objects '
                            'were verified as deletable.',
                            log=log
                        )
                    else:
                        logprint(
                            f'Current zip objects (with matching prefix): {len_cz} '
                            'would be deleted.',
                            log=log
                        )
                    sys.exit()
                else:
                    logprint(f'Current objects (with matching prefix): {len_co}')
                    if not verify:
                        logprint(
                            f'Current zip objects (with matching prefix): {len_cz} will be '
                            'deleted.'
                        )
                    else:
                        logprint(
                            f'Current zip objects (with matching prefix): {len_cz} will be '
                            'deleted if all contents exist as objects.'
                        )
                    logprint('WARNING! Files are about to be deleted!')
                    logprint('Continue [y/n]?')
                    if not yes:
                        if input().lower() != 'y':
                            sys.exit()
                    else:
                        logprint('auto y')

                    if verify:
                        current_zips['DELETED'] = current_zips[current_zips['verified'] == True].map_partitions(  # noqa
                            lambda partition: partition.apply(
                                delete_object_swift,
                                axis=1,
                                args=(
                                    s3,
                                    bucket_name,
                                    True,
                                    log,
                                ),
                            ),
                            meta=('bool')
                        )
                        current_zips[current_zips['verified'] == False]['DELETED'] = False  # noqa
                    else:
                        current_zips['DELETED'] = current_zips.map_partitions(
                            lambda partition: partition.apply(
                                delete_object_swift,
                                axis=1,
                                args=(
                                    s3,
                                    bucket_name,
                                    True,
                                    log
                                )
                            ),
                            meta=('bool')
                        )

                    current_zips = current_zips.compute()

                    if verify:
                        logprint(
                            f'{len(current_zips[current_zips["verified"] == True])} zip files were verified.',
                            log=log
                        )
                        logprint(
                            f'{len(current_zips[current_zips["DELETED"] == True])} zip files were DELETED.',
                            log=log
                        )
                        logprint(
                            f"{len(current_zips[current_zips['verified'] == False])} zip files were not "
                            "verified and not deleted.",
                            log=log
                        )
                        if len(
                            current_zips[current_zips['verified'] == True]  # noqa
                        ) != len(
                            current_zips[current_zips['DELETED'] == True]  # noqa
                        ):
                            logprint(
                                "Some errors may have occurred, as some zips verified for deletion were not "
                                "deleted.",
                                log=log
                            )
                    else:
                        logprint(
                            f'{len(current_zips[current_zips["DELETED"] == True])} zip files were DELETED.',
                            log=log
                        )
                    logprint(
                        f'Finished processing at {datetime.now()}, elapsed time = {datetime.now() - start}',
                        log=log
                    )
                    sys.exit(0)
            else:
                print(f'No zip files in bucket {bucket_name}.')
            if len_md > 0 and clean_metadata:
                if dryrun:
                    logprint(
                        'Any orphaned metadata files would be found and deleted.',
                        log=log
                    )
                else:
                    cleaned_metadata = current_objects.map_partitions(
                        lambda partition: partition.apply(
                            clean_orphaned_metadata,
                            axis=1,
                            args=(
                                s3,
                                bucket_name,
                                current_objects['CURRENT_OBJECTS'],
                                log
                            )
                        ),
                        meta=('bool')
                    )
                    cleaned_metadata = cleaned_metadata.compute()
                    logprint(
                        f'{len(cleaned_metadata[cleaned_metadata == True])} '
                        'orphaned metadata files were DELETED.',
                        log=log
                    )
        else:
            print(f'No files in bucket {bucket_name}. Exiting.')
            sys.exit(0)
