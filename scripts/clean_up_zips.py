import gc
import sys
import os
import warnings
import pandas as pd
from psutil import virtual_memory as mem
import bucket_manager.bucket_manager as bm
import swiftclient.exceptions
import swiftclient
import argparse
from dask import dataframe as dd
from distributed import Client
from dask_kubernetes.operator import KubeCluster
import subprocess
import logging
from random import randint
from string import ascii_lowercase as letters
from multiprocessing import cpu_count
warnings.filterwarnings('ignore')


NAMESPACE_FILE = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'


# Check if the namespace file exists and read it
def get_current_namespace():
    """Reads the namespace from the file system if available."""
    if os.path.exists(NAMESPACE_FILE):
        with open(NAMESPACE_FILE, 'r') as f:
            return f.read().strip()
    return "default"  # Fallback


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
            logprint(f'Deleted {obj} as {zip_obj} does not exist', 'info')
        except swiftclient.exceptions.ClientException as e:
            logprint(f'WARNING: Error deleting {obj}: {e.msg}', 'warning')
        return True
    else:
        return False


def explode_zip_contents(df: pd.DataFrame, s3: swiftclient.Connection, bucket_name: str) -> pd.DataFrame:
    """
    Explodes zip file entries in a DataFrame by retrieving and parsing their metadata.

    Parameters
    ----------
    row : pd.Series
        Row of dd.dataframe with a column `CURRENT_OBJECTS` containing S3 object keys.
    s3 : swiftclient.Connection
        Authenticated Swift client connection used to fetch metadata objects.
    bucket_name : str
        Name of the target bucket where zip files and corresponding metadata files are stored.

    Returns
    -------
    pd.DataFrame
        A DataFrame with columns:
          - zip_filename (str): The original ZIP object key.
          - content_filename (str or None): The full path to each item inside the ZIP, or None if retrieval
            failed.
          - total_contents (int): Total number of entries in the ZIP, or -1 on failure to fetch metadata.
    """
    output_rows = []
    for row in df.itertuples():
        zip_obj = row.CURRENT_OBJECTS
        if not zip_obj.endswith('.zip') or not isinstance(zip_obj, str):
            continue  # Skip if not a valid zip file name
        logprint(f'Exploding contents of {zip_obj}', 'info')
        path_stub = '/'.join(zip_obj.split('/')[:-1])
        zip_metadata_uri = f'{zip_obj}.metadata'
        try:
            metadata = s3.get_object(bucket_name, zip_metadata_uri)[1]
            contents = [f'{path_stub}/{c}' for c in metadata.decode().split('|') if c]
            for content in contents:
                output_rows.append({
                    'zip_filename': zip_obj,
                    'content_filename': content,
                    'total_contents': len(contents)
                })
        except swiftclient.exceptions.ClientException as e:
            logprint(f'WARNING: Error getting metadata for {zip_obj}: {e.msg}', 'warning')
            output_rows.append({
                'zip_filename': zip_obj,
                'content_filename': None,
                'total_contents': -1
            })
    if not output_rows:
        return pd.DataFrame(columns=['zip_filename', 'content_filename', 'total_contents'])
    return pd.DataFrame(output_rows)


if __name__ == '__main__':

    logger = logging.getLogger('clean_zips')

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
    parser.add_argument(
        '--dask-workers',
        '-w',
        type=int,
        default=4,
        help='Number of Dask workers to use. Default is 4.'
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

    dryrun = args.dryrun
    yes = args.yes
    verify = not args.verify_skip  # Default is to verify
    clean_metadata = args.clean_up_metadata
    num_threads = args.nthreads
    dask_workers = args.dask_workers
    logprint(
        f'API: {api}, Bucket name: {bucket_name}, Prefix: {prefix}, '
        f'Number of Dask workers: {dask_workers}, Number of threads per worker: {num_threads}, '
        f'dryrun: {dryrun}',
        'info'
    )
    if num_threads * dask_workers > cpu_count() - 8:
        logprint(
            f'FATAL: Total number of threads ({num_threads * dask_workers}) '
            f'exceeds available CPU cores ({cpu_count() - 8}).',
            'error'
        )
        sys.exit(1)

    # Print hostname
    uname = subprocess.run(['uname', '-n'], capture_output=True)
    logprint(f'Running on {uname.stdout.decode().strip()}', 'info')

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

    logprint(f'Using namespace: {namespace}', 'info')
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

    # Process the files
    with Client(cluster) as client:
        logprint(f'Dask Client: {client}', 'info')
        logprint(f'Dashboard: {client.dashboard_link}', 'info')
        logprint(
            'Starting processing.',
            'info'
        )
        logprint(f'Using {dask_workers} Dask workers.', 'info')
        logprint(
            f'Getting current object list for {bucket_name}. '
            'This is necessarily serial and may take some time. Starting.',
            'info'
        )

        if api == 's3':
            current_object_names = bm.object_list(bucket, prefix=prefix, count=False)
        elif api == 'swift':
            current_object_names = bm.object_list_swift(s3, bucket_name, prefix=prefix, count=False)
        current_object_names = pd.DataFrame.from_dict({'CURRENT_OBJECTS': current_object_names})
        logprint('Done.', 'info')

        current_objects = dd.from_pandas(
            current_object_names,
            npartitions=1000 * dask_workers
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
            current_objects['is_zip'] = current_objects['CURRENT_OBJECTS'].str.fullmatch(
                zip_match,
                na=False
            )
            current_objects['is_metadata'] = current_objects['CURRENT_OBJECTS'].str.fullmatch(
                metadata_match,
                na=False
            )

            logprint('Reducing current_objects to only zip files.', 'info')
            current_zips = current_objects[current_objects['is_zip'] == True].persist()  # noqa

            logprint('Persisting current_zips.', 'debug')

            num_cz = current_zips.shape[0].compute()  # noqa
            remaining_objects = current_objects[(current_objects['is_zip'] == False) & (current_objects['is_metadata'] == False)]['CURRENT_OBJECTS']  # noqa

            logprint(f'Persisted current_zips, len: {num_cz}', 'debug')
            logprint(f'Current_zips Partitions: {current_zips.npartitions}', 'debug')

            logprint(
                f'Found {num_cz} zip files (with matching prefix) in bucket {bucket_name}.',
                'info'
            )

            if num_cz > 0:
                logprint('Verifying zips can be deleted (i.e., whether contents exist).', 'info')
                if verify:
                    logprint('Reading contents from all zip metadata files.', 'info')
                    exploded_contents = current_zips.map_partitions(
                        explode_zip_contents,
                        s3=s3,
                        bucket_name=bucket_name,
                        meta={
                            'zip_filename': 'object',
                            'content_filename': 'object',
                            'total_contents': 'int64'
                        },
                    )
                    logprint('Merging zip contents with list of existing objects.', 'info')
                    # ensurce remaining_objects is a Dask DataFrame not a Dask Series
                    remaining_objects_df = remaining_objects.to_frame(name='CURRENT_OBJECTS')
                    verified_contents = dd.merge(
                        exploded_contents,
                        remaining_objects_df,
                        left_on='content_filename',
                        right_on='CURRENT_OBJECTS',
                        how='inner'
                    )

                    logprint('Counting matched files to identify fully verified zips.', 'info')
                    # Count how many contents were found for each zip
                    verified_counts = verified_contents.groupby(
                        'zip_filename'
                    ).content_filename.count().persist()
                    # Get the original total number of contents for each zip
                    total_counts = exploded_contents.groupby(
                        'zip_filename'
                    ).total_contents.first().persist()

                    # Align the two series. A zip might be in total_counts but not in
                    # verified_counts if none of its contents were found. Reindex
                    # verified_counts to match the full index of total_counts, filling
                    # any zips that had 0 found files with the value 0.
                    verified_counts = verified_counts.reindex(total_counts.index.compute(), fill_value=0)

                    # A zip is verified if the number of found files equals the total number of files
                    verified_zips_series = (verified_counts.compute() == total_counts.compute())
                    verified_zips_df = verified_zips_series[verified_zips_series].reset_index()
                    verified_zips_df.columns = ['CURRENT_OBJECTS', 'verified']

                    logprint('Step 4: Merging verification results back into main zip list.', 'info')
                    # We do a left merge to keep all original zips, verified will be NaN for non-verified ones
                    current_zips = dd.merge(
                        current_zips,
                        verified_zips_df,
                        on='CURRENT_OBJECTS',
                        how='left'
                    ).fillna({'verified': False})  # Mark non-verified zips as False instead of NaN

                if dryrun:
                    logprint(f'Current objects (with matching prefix): {num_co}', 'info')
                    if verify:
                        pass
                        current_zips = client.persist(current_zips)
                        logprint(
                            f'{client.compute(len(current_zips[current_zips["verified"] == True]))} zip '
                            'objects were verified as deletable.',
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
                            ),
                        ),
                        meta=('bool'),
                    )
                    logprint('Deleting zip files.', 'info')
                    logprint('Computing deleted.', 'debug')
                    num_d = current_zips['DELETED'].sum().compute()  # noqa

                    if verify:
                        logprint(
                            f'{num_d} zip files were DELETED.',
                            'info'
                        )
                    else:
                        logprint(
                            f'{num_d} zip files were DELETED.',
                            'info'
                        )
                    logprint('Finished zip deletion.', 'info')

            else:
                logprint(f'No zip files in bucket {bucket_name}.', 'warning')

            if clean_metadata:
                logprint('Cleaning up orphaned metadata files currently disabled.', 'info')
            # if clean_metadata:
            #     logprint('Checking for orphaned metadata files.', 'info')
            #     current_objects = dd.from_pandas(
            #         pd.DataFrame.from_dict(
            #             {'CURRENT_OBJECTS': bm.object_list_swift(s3, bucket_name, prefix=prefix, count=False)}
            #         ),
            #         npartitions=1000 * dask_workers
            #     )
            #     logprint(
            #         'Done.',
            #         'info'
            #     )
            #     print('this is line 739', flush=True)
            #     current_objects['is_metadata'] = current_objects['CURRENT_OBJECTS'].map_partitions(
            #         lambda partition: partition.str.fullmatch(metadata_match, na=False)
            #     )
            #     zip_check = True
            #     if 'is_zip' in current_objects.columns:
            #         current_zip_names = client.persist(current_objects[current_objects['is_zip'] == True]['CURRENT_OBJECTS'])  # noqa
            #     else:
            #         zip_check = False
            #     md_objects = client.persist(current_objects[current_objects['is_metadata'] == True])  # noqa
            #     len_md = len(md_objects)
            #     if len_md > 0:  # noqa
            #         md_objects = dd.from_pandas(md_objects, npartitions=1000 * dask_workers)  # noqa
            #         if dryrun:
            #             logprint(
            #                 'Any orphaned metadata files would be found and deleted.',
            #                 'info'
            #             )
            #         else:
            #             if zip_check:
            #                 md_objects['ORPHANED'] = md_objects.map_partitions(
            #                     lambda partition: partition.apply(
            #                         is_orphaned_metadata,
            #                         axis=1,
            #                         args=(
            #                             current_zip_names,
            #                         )
            #                     ),
            #                     meta=('bool')
            #                 )
            #             else:
            #                 md_objects['ORPHANED'] = md_objects.map_partitions(
            #                     lambda partition: partition.apply(
            #                         lambda row: row['CURRENT_OBJECTS'].endswith('.zip.metadata'),
            #                         axis=1
            #                     ),
            #                     meta=('bool')
            #                 )
            #             md_objects['deleted'] = md_objects.map_partitions(  # noqa
            #                 lambda partition: partition.apply(
            #                     clean_orphaned_metadata,
            #                     axis=1,
            #                     args=(
            #                         s3,
            #                         bucket_name,
            #                         # logger,
            #                     )
            #                 ),
            #                 meta=('bool')
            #             )
            #             md_objects = client.persist(md_objects)
            #             logprint(
            #                 f'{len(md_objects["deleted" == True])} '  # noqa
            #                 'orphaned metadata files were DELETED.',
            #                 'info'
            #             )
            #     else:
            #         logprint(
            #             f'No metadata files in bucket {bucket_name}.',
            #             'warning'
            #         )
        else:
            logprint(f'No files in bucket {bucket_name}. Exiting.', 'warning')
            sys.exit(0)
    logprint('Cleaning up Dask cluster.', 'info')
    cluster.close()

sys.exit(0)
