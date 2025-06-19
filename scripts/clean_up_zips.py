import gc
import sys
import os
import warnings
from datetime import datetime
import pandas as pd
from psutil import virtual_memory as mem
import bucket_manager.bucket_manager as bm
import swiftclient.exceptions
import swiftclient
import argparse
import dask
from dask import dataframe as dd
from dask import delayed
from distributed import Client
import subprocess
import logging
warnings.filterwarnings('ignore')


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def logprint(msg: str, log=None) -> None:
    """
    Logs a message to a specified log file or prints it to the console.

    Parameters:
    msg (str): The message to be logged or printed.

    log (str, optional): The file path to the log file. If None, the message
        is printed to the console. Defaults to None.

    Returns:
    None
    """
    if isinstance(log, bool):
        if log:
            log = None
        else:
            return
    if log is not None:
        with open(log, 'a') as logfile:
            logfile.write(f'{msg}\n')
    else:
        print(msg, flush=True)
    return None


@dask.delayed
def delete_object_swift(
    row: pd.Series,
    s3: swiftclient.Connection,
    bucket_name: str,
    del_metadata: bool = True,
    verify: bool = False,
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
    prints = []
    if verify:
        if row['verified'] is False:
            logprint(f'WARNING: {row["CURRENT_OBJECTS"]} not verified for deletion.', log)
            return False
    obj = row['CURRENT_OBJECTS']
    if pd.isna(obj):
        # logprint('WARNING: obj is NaN', log)
        return False
    # logprint('DEBUG:', log)
    # logprint(f'Row: {row}', log)
    # logprint(f'Object to delete: {obj}', log)
    # logprint(f'type(obj): {type(obj)}', log)
    deleted = False
    try:
        s3.delete_object(bucket_name, obj)
        prints.append(delayed(logprint(f'Deleted {obj}', log)))
        deleted = True
    except Exception as e:
        print(f'Error deleting {obj}: {e}', file=sys.stderr)
        return False
    if del_metadata:
        try:
            s3.delete_object(bucket_name, f'{obj}.metadata')
            prints.append(delayed(logprint(f'Deleted {obj}.metadata', log)))
        except swiftclient.exceptions.ClientException as e:
            prints.append(delayed(logprint(f'WARNING: Error deleting {obj}.metadata: {e.msg}', log)))
    dask.compute(*prints)
    return deleted


@dask.delayed
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


@dask.delayed
def clean_orphaned_metadata(
    row: pd.Series,
    s3: swiftclient.Connection,
    bucket_name: str,
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


@dask.delayed
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
    if zip_obj == 'None':
        logprint(f'WARNING: {zip_obj} is None', log)
        return False
    path_stub = '/'.join(zip_obj.split('/')[:-1])
    zip_metadata_uri = f'{zip_obj}.metadata'
    # logprint(
    #     f'DEBUG:\nzip_obj: {zip_obj}\npath_stub: {path_stub}\n'
    #     f'zip_metadata_uri: {zip_metadata_uri}\n'
    # )
    # logprint(f'Row: {row}', log)
    # logprint(f'Current objects: {len(current_objects)}', log)
    try:
        zip_metadata = s3.get_object(bucket_name, zip_metadata_uri)[1]
    except swiftclient.exceptions.ClientException as e:
        logprint(f'WARNING: Error getting {zip_metadata_uri}: {e.msg}', log)
        return False

    contents = [f'{path_stub}/{c}' for c in zip_metadata.decode().split('|') if c]
    lc = len(contents)
    verified = False

    # logprint(f'Contents: {lc}', log)
    try:
        if sum(current_objects.isin(contents).values) == lc:
            verified = True
            logprint(f'{zip_obj} verified: {verified} - can be deleted', log)
        else:
            verified = False
            logprint(f'{zip_obj} verified: {verified} - cannot be deleted', log)
    except Exception as e:
        logprint(f'Error verifying {zip_obj}: {e}', log)
        verified = False
    del zip_metadata, contents
    gc.collect()
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
    num_threads = args.nthreads
    if num_threads > nprocs:
        print(f'Number of threads ({num_threads}) cannot be greater than number of processes ({nprocs}). '
              f'Setting threads per worker to {nprocs // 2}.', file=sys.stderr)
        num_threads = nprocs // 2

    print(f'API: {api}, Bucket name: {bucket_name}, Prefix: {prefix}, nprocs: {nprocs}, dryrun: {dryrun}')

    # Set up logging
    if log_to_file:
        log = f'clean_zips_{bucket_name}_{"-".join(prefix.split("/"))}'
        f'_{datetime.now().strftime("%Y%m%d%H%M%S")}.log'
        if not os.path.exists(log):
            logprint(f'Created log file {log}', log)
        logprint(log, log)
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
        sys.exit('AssertionError occurred. Exiting.')
    except KeyError as e:
        print(f'KeyError {e}', file=sys.stderr)
        sys.exit('KeyError occurred. Exiting.')
    except ValueError as e:
        print(f'ValueError {e}', file=sys.stderr)
        sys.exit('ValueError occurred. Exiting.')

    logprint(f'Using {api.capitalize()} API with host {s3_host}')

    if api == 's3':
        print('Currently only Swift is supported for parallelism with Dask. Exiting.', file=sys.stderr)
        sys.exit('Currently only Swift is supported for parallelism with Dask. Exiting.')

    elif api == 'swift':
        s3 = bm.get_conn_swift()
        bucket_list = bm.bucket_list_swift(s3)

    if bucket_name not in bucket_list:
        print(f'Bucket {bucket_name} not found in {api} bucket list. Exiting.', file=sys.stderr)
        sys.exit(f'Bucket {bucket_name} not found in {api} bucket list. Exiting.')

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

    logprint(f'nprocs: {nprocs}, Threads per worker: {num_threads}, Number of workers: {n_workers}, '
             f'Total memory: {total_memory/1024**3:.2f} GiB, Memory per worker: '
             f'{mem_per_worker/1024**3:.2f} GiB')

    # Process the files
    with Client(
        n_workers=n_workers,
        threads_per_worker=num_threads,
        memory_limit=f'{(mem().total//1024**3)*7//8}GB'
    ) as client:
        logprint(f'Dask Client: {client}', log=log)
        logprint(f'Dashboard: {client.dashboard_link}', log=log)
        logprint(f'Starting processing at {datetime.now()}, elapsed time = {datetime.now() - start}', log=log)
        logprint(f'Using {nprocs} processes.', log=log)
        logprint(f'Getting current object list for {bucket_name}. This may take some time.\nStarting at '
                 f'{datetime.now()}, elapsed time = {datetime.now() - start}', log=log)

        if api == 's3':
            current_object_names = bm.object_list(bucket, prefix=prefix, count=False)
        elif api == 'swift':
            current_object_names = bm.object_list_swift(s3, bucket_name, prefix=prefix, count=False)
        current_object_names = pd.DataFrame.from_dict({'CURRENT_OBJECTS': current_object_names})
        logprint(f'Done.\nFinished at {datetime.now()}, elapsed time = {datetime.now() - start}', log=log)
        len_co = len(current_object_names)
        current_objects = dd.from_pandas(
            current_object_names,
            npartitions=n_workers * 100
        )
        # del current_object_names
        # nparts = current_objects.npartitions
        # use_nparts = max(
        #     nparts // nparts % n_workers, n_workers, nparts // n_workers
        # ) // n_workers * n_workers
        # if use_nparts != nparts:
        #     current_objects = current_objects.repartition(npartitions=use_nparts)
        logprint(f'Current_objects Partitions: {current_objects.npartitions}', log=log)

        logprint(f'Found {len(current_objects)} objects (with matching prefix) in bucket {bucket_name}.',
                 log=log)
        if len_co > 0:
            zip_match = r".*collated_\d+\.zip$"
            metadata_match = r".*collated_\d+\.zip\.metadata$"
            current_objects['is_zip'] = current_objects['CURRENT_OBJECTS'].map_partitions(
                lambda partition: partition.str.fullmatch(zip_match, na=False)  # noqa
            )
            # current_objects['is_metadata'] = current_objects['CURRENT_OBJECTS'].map_partitions(
            #     lambda partition: partition.str.fullmatch(metadata_match, na=False)  # noqa
            # )
            current_zips = client.persist(current_objects[current_objects['is_zip'] == True])  # noqa
            len_cz = len(current_zips)  # noqa
            logprint(
                f'Found {len_cz} zip files (with matching prefix) in bucket {bucket_name}.',
                log=log
            )

            # md_objects = dd.from_pandas(current_objects[current_objects['is_metadata'] == True].compute())  # noqa
            # len_md = len(md_objects)
            # logprint(
            #     f'Found {len_md} metadata files (with matching prefix) in bucket {bucket_name}.',
            #     log=log
            # )

            # if not verify:
            #     del current_objects

            # print(f'n_partitions: {current_zips.npartitions}')

            if len_cz > 0:
                logprint('Verifying zips can be deleted (i.e., contents exist).')
                # current_objects = current_objects['CURRENT_OBJECTS'].compute()  # noqa
                # client.scatter(current_object_names, broadcast=True)  # noqa
                # current_object_names = client.persist(current_objects['CURRENT_OBJECTS'])  # noqa
                if verify:
                    current_zips['verified'] = current_zips.map_partitions(
                        lambda partition: partition.apply(
                            verify_zip_objects,
                            axis=1,
                            args=(
                                s3,
                                bucket_name,
                                current_object_names,
                                log
                            ),
                        ),
                        meta=('bool'),
                    )
                    # del current_object_names
                if dryrun:
                    logprint(f'Current objects (with matching prefix): {len_co}', log=log)
                    if verify:
                        current_zips = client.persist(current_zips)
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
                            sys.exit('Please supply a valid answer. Exiting.')
                    else:
                        logprint('auto y')
                    # current_zips = current_zips.dropna(subset=['CURRENT_OBJECTS'])  # noqa
                    # if verify:
                    # current_zips['DELETED'] = current_zips.map_partitions(  # noqa
                    #     lambda partition: partition.apply(

                    logprint('Preparing to delete zip files.', log=log)
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
                                log,
                            ),
                        ),
                        meta=('bool'),
                    )
                        # current_zips[current_zips['verified'] == False]['DELETED'] = False  # noqa
                    # else:
                    #     current_zips['DELETED'] = current_zips.map_partitions(
                    #         lambda partition: partition.apply(
                    #             delete_object_swift,
                    #             axis=1,
                    #             args=(
                    #                 s3,
                    #                 bucket_name,
                    #                 True,
                    #                 log
                    #             )
                    #         ),
                    #         meta=('bool')
                    #     )

                    current_zips = client.persist(current_zips)

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
                    del current_zips
                    logprint(
                        f'Finished processing at {datetime.now()}, elapsed time = {datetime.now() - start}',
                        log=log
                    )
                    # sys.exit(0)
            else:
                print(f'No zip files in bucket {bucket_name}.')

            if clean_metadata:
                logprint('Checking for orphaned metadata files.', log=log)
                current_objects = dd.from_pandas(
                    pd.DataFrame.from_dict(
                        {'CURRENT_OBJECTS': bm.object_list_swift(s3, bucket_name, prefix=prefix, count=False)}
                    ),
                    chunksize=100000
                )
                logprint(
                    f'Done.\nFinished at {datetime.now()}, elapsed time = {datetime.now() - start}', log=log
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
                            log=log
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
                                    log
                                )
                            ),
                            meta=('bool')
                        )
                        md_objects = client.persist(md_objects)
                        logprint(
                            f'{len(md_objects["deleted" == True])} '  # noqa
                            'orphaned metadata files were DELETED.',
                            log=log
                        )
                else:
                    logprint(
                        f'No metadata files in bucket {bucket_name}.',
                        log=log
                    )
        else:
            print(f'No files in bucket {bucket_name}. Exiting.')
            sys.exit(0)
