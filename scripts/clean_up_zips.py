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


def delete_object_swift(obj: str, s3: swiftclient.Connection, log: str = None) -> bool:
    """
    Deletes an object and its metadata from an S3 bucket.

    Args:
        obj (str): The name of the object to delete.

        s3 (object): The S3 client object used to perform the deletion.

        log (function, optional): A logging function to record messages.
        Defaults to None.

    Returns:
        bool: True if the object was successfully deleted, False otherwise.

    Raises:
        Exception: If there is an error deleting the object.

        swiftclient.exceptions.ClientException: If there is an error deleting
        the object's metadata.
    """
    deleted = False
    try:
        s3.delete_object(bucket_name, obj)
        logprint(f'Deleted {obj}', log)
        deleted = True
    except Exception as e:
        print(f'Error deleting {obj}: {e}', file=sys.stderr)
        return False
    try:
        s3.delete_object(bucket_name, f'{obj}.metadata')
        logprint(f'Deleted {obj}.metadata', log)
    except swiftclient.exceptions.ClientException as e:
        logprint(f'WARNING: Error deleting {obj}.metadata: {e.msg}', log)
    return deleted


def verify_zip_objects(
    zip_obj: str,
    s3: swiftclient.Connection,
    bucket_name: str,
    current_objects: pd.Series,
    log: str
) -> bool:
    """
    Verifies the contents of a zip file based on the provided row and keys
    series.

    Args:
        row (pd.Series): A pandas Series containing information about the zip
        file, including 'key', 'is_zipfile', and 'contents'.

        keys_series (pd.Series): A pandas Series containing keys to check
        against the zip file contents.

    Returns:
        bool: True if the zip file needs to be extracted, False otherwise.
    """
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
        '--dryrun',
        '-d',
        default=False,
        action='store_true',
        help='Perform a dry run without uploading files. Default is False.'
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
        '--verify',
        '-v',
        default=False,
        action='store_true',
        help='Verify the contents of the zip file are in the list of uploaded files. Default is False.'
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
    verify = args.verify

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

        current_objects = pd.DataFrame.from_dict({'CURRENT_OBJECTS': current_objects})
        logprint(f'Found {len(current_objects)} objects (with matching prefix) in bucket {bucket_name}.',
                 log=log)
        if not current_objects.empty:
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
            ].copy()
            logprint(
                f'Found {len(current_zips)} zip files (with matching prefix) in bucket {bucket_name}.',
                log=log
            )
            if len(current_zips) > 0:
                if verify:
                    current_zips = dd.from_pandas(current_zips, chunksize=10000)
                    current_zips['verified'] = current_zips['CURRENT_OBJECTS'].apply(
                        lambda x: verify_zip_objects(
                            x,
                            s3,
                            bucket_name,
                            current_objects['CURRENT_OBJECTS'],
                            log
                        ),
                        meta=('bool')
                    )
                if dryrun:
                    logprint(f'Current objects (with matching prefix): {len(current_objects)}', log=log)
                    if verify:
                        current_zips = current_zips.compute()
                        logprint(
                            f'{len(current_zips[current_zips["verified"] == True])} zip objects '
                            'were verified as deletable.',
                            log=log
                        )
                    else:
                        logprint(
                            f'Current zip objects (with matching prefix): {len(current_zips)} '
                            'would be deleted.',
                            log=log
                        )
                    sys.exit()
                else:
                    logprint(f'Current objects (with matching prefix): {len(current_objects)}')
                    if not verify:
                        logprint(
                            f'Current zip objects (with matching prefix): {len(current_zips)} will be '
                            'deleted.'
                        )
                    else:
                        logprint(
                            f'Current zip objects (with matching prefix): {len(current_zips)} will be '
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
                        current_zips['DELETED'] = current_zips[current_zips['verified'] == True][  # noqa
                            'CURRENT_OBJECTS'
                        ].apply(lambda x: delete_object_swift(x, s3, log), meta=('bool'))
                        current_zips[current_zips['verified'] == False]['DELETED'] = False  # noqa
                    else:
                        current_zips['DELETED'] = current_zips['CURRENT_OBJECTS'].apply(
                            lambda x: delete_object_swift(
                                x,
                                s3,
                                log
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
                print(f'No zip files in bucket {bucket_name}. Exiting.')
                sys.exit(0)
        else:
            print(f'No files in bucket {bucket_name}. Exiting.')
            sys.exit(0)
