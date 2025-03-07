#!/usr/env/python

"""
Helper functions for using an s3 bucket
"""
import boto3
import os
from botocore.exceptions import ClientError
import swiftclient
import swiftclient.service

def print_buckets(resource) -> None:
    """
    Prints the names of all buckets in the S3 endpoint.

    Parameters:
    - resource: The S3 resource object.

    Returns:
    None
    """
    for b in resource.buckets.all():
        print(b.name)

def check_keys(api: str ='swift') -> None:
    """
    Retrieves the access key and secret key for the specified API.

    Parameters:
    - api: The API to retrieve the keys for. Can be 'S3' or 'Swift'.

    Returns:
    For S3 API: a dictionary containing the access key and secret key.
    For Swift API: a dictionary containing the user and secret key.
    """
    if api == 's3':
        try:
            assert 'S3_ACCESS_KEY' in os.environ
            assert 'S3_SECRET_KEY' in os.environ
            assert 'S3_HOST_URL' in os.environ
        except AssertionError:
            raise AssertionError('Set S3_ACCESS_KEY, S3_SECRET_KEY and S3_HOST_URL environment variables.')
        return True

    elif api == 'swift':
        try:
            assert 'ST_USER' in os.environ
            assert 'ST_KEY' in os.environ
            assert 'ST_AUTH' in os.environ
        except AssertionError:
            raise AssertionError('Set ST_USER, ST_KEY and ST_AUTH environment variables.')
        return True
    else:
        raise ValueError(f'Invalid API: {api}')

def get_resource():
    """
    Creates and returns an S3 resource object for the specified S3 endpoint.

    Requires the following environment variables to be set:
    - S3_ACCESS_KEY: The S3 access key.
    - S3_SECRET_KEY: The S3 secret key.
    - S3_HOST_URL: The S3 endpoint URL.

    Returns:
    An S3 resource object.
    """
    try:
        creds = {'access_key': os.environ['S3_ACCESS_KEY'],
            'secret_key': os.environ['S3_SECRET_KEY'],
            'host_url': os.environ['S3_HOST_URL']}
    except KeyError as e:
        raise KeyError('Set S3_ACCESS_KEY, S3_SECRET_KEY and S3_HOST_URL environment variables.')
    session = boto3.Session(
        aws_access_key_id=creds['access_key'],
        aws_secret_access_key=creds['secret_key']
    )
    return session.resource(
        service_name='s3',
        endpoint_url=creds['host_url'],
        verify=False  # Disable SSL verification for non-AWS S3 endpoints
    )

def get_client():
    """
    Creates and returns an S3 client object for the specified S3 endpoint.

    Requires the following environment variables to be set:
    - S3_ACCESS_KEY: The S3 access key.
    - S3_SECRET_KEY: The S3 secret key.
    - S3_HOST_URL: The S3 endpoint URL.

    Returns:
    An S3 client object.
    """
    try:
        creds = {'access_key': os.environ['S3_ACCESS_KEY'],
            'secret_key': os.environ['S3_SECRET_KEY'],
            'host_url': os.environ['S3_HOST_URL']}
    except KeyError as e:
        raise KeyError('Set S3_ACCESS_KEY, S3_SECRET_KEY and S3_HOST_URL environment variables.')
    session = boto3.Session(
        aws_access_key_id=creds['access_key'],
        aws_secret_access_key=creds['secret_key']
    )
    return session.client(
        service_name='s3',
        endpoint_url=creds['host_url'],
        verify=False  # Disable SSL verification for non-AWS S3 endpoints
    )

def bucket_list(resource) -> list[str]:
    """
    Returns a list of bucket names in the S3 endpoint.

    Parameters:
    - resource: The S3 resource object.

    Returns:
    A list of bucket names.
    """
    return [ b.name for b in resource.buckets.all() ]

def bucket_list_swift(conn: swiftclient.Connection) -> list[str]:
    """
    Returns a list of container names in the Swift S3 endpoint.

    Parameters:
    - conn: swiftclient.client.Connection object.

    Returns:
    A list of container names.
    """
    return [ c['name'] for c in conn.get_account()[1] ]

def create_bucket(resource, bucket_name: str) -> bool:
    """
    Creates a new bucket in the S3 endpoint.

    Parameters:
    - resource: The S3 resource object.

    Returns:
    True if the bucket was created successfully, False otherwise.
    """
    try:
        resource.create_bucket(Bucket=bucket_name)
    except Exception as e:
        print(e)
    return True

def print_objects(bucket) -> None:
    """
    Prints the keys of all objects in the specified bucket.

    Parameters:
    - bucket: The S3 bucket object.

    Returns:
    None
    """
    for obj in bucket.objects.all():
        print(obj.key)

def object_list(bucket, prefix='', count=False) -> list[str]:
    """
    Returns a list of keys of all objects in the specified bucket.

    Parameters:
    - bucket: The S3 bucket object.

    Returns:
    A list of object keys.
    """
    keys = []
    if count:
        o = 0
    for obj in bucket.objects.filter(Prefix=prefix):
        keys.append(obj.key)
        if count:
            o += 1
            if o % 10000 == 0:
                print(f'Existing objects: {o}', end='\r', flush=True)
    return keys

def object_list_swift(conn: swiftclient.Connection, container_name: str, prefix : str = None, full_listing: bool = True, count: bool = False) -> list[str]:
    """
    Returns a list of keys of all objects in the specified bucket.

    Parameters:
    - conn: swiftlient.Connection object.
    - container_name: The name of the Swift container.

    Returns:
    A list of object keys.
    """
    keys = []
    if count:
        o = 0
    for obj in conn.get_container(container_name,prefix=prefix,full_listing=full_listing)[1]:
        keys.append(obj['name'])
        if count:
            o += 1
            if o % 10000 == 0:
                print(f'Existing objects: {o}', end='\r', flush=True)
    return keys

def print_containers_swift(conn: swiftclient.Connection) -> None:
    """
    Prints the names of all containers in the Swift endpoint.

    Parameters:
    - conn: The Swift connection object.

    Returns:
    None
    """
    for container in conn.get_account()[1]:
        print(container['name'])

def print_contents_swift(conn: swiftclient.Connection, container_name: str) -> None:
    """
    Prints the names, sizes, and last modified timestamps of all objects in the specified container.

    Parameters:
    - container_name: The name of the Swift container.

    Returns:
    None
    """
    for data in conn.get_container(container_name)[1]:
        print('{0}\t{1}\t{2}'.format(data['name'], data['bytes'], data['last_modified']))

def get_conn_swift() -> swiftclient.Connection:
    """
    Creates and returns a Swift connection object for the specified Swift endpoint.

    Requires the following environment variables to be set:
    - ST_USER: The Swift user.
    - ST_KEY: The Swift secret key.
    - ST_AUTH: The Swift authentication URL.

    Returns:
    A Swift connection object.
    """
    try:
        creds = {'user': os.environ['ST_USER'],
            'key': os.environ['ST_KEY'],
            'authurl': os.environ['ST_AUTH']}
    except KeyError as e:
        raise KeyError('Set ST_USER, ST_KEY and ST_AUTH environment variables.')
    return swiftclient.Connection(
        user=creds['user'],
        key=creds['key'],
        authurl=creds['authurl']
    )

def get_service_swift() -> swiftclient.service.SwiftService:
    """
    Creates and returns a Swift service object for the specified Swift endpoint.

    Parameters:
    - user: The Swift user.
    - secret_key: The Swift secret key.
    - host: The Swift authentication URL.

    Returns:
    A Swift service object.
    """
    try:
        creds = {'user': os.environ['ST_USER'],
             'key': os.environ['ST_KEY'],
             'authurl': os.environ['ST_AUTH']}
    except KeyError as e:
        raise KeyError('Set ST_USER, ST_KEY and ST_AUTH environment variables.')
    return swiftclient.service.SwiftService(
        {
            'auth_version': '1',
            'os_auth_url': creds['authurl'],
            'os_username': creds['user'].split(':')[1],
            'os_password': creds['key'],
            'os_project_name': creds['user'].split(':')[0],
            'os_user_domain_name': 'default',
            'os_project_domain_name': 'default'
        }
    )

def get_SwiftUploadObject(source, object_name=None, options=None) -> swiftclient.service.SwiftUploadObject:
    return swiftclient.service.SwiftUploadObject(source, object_name, options)

def download_file_swift(conn: swiftclient.Connection, container_name: str, object_name: str, local_path: str) -> None:
    """
    Downloads a file from the specified container.

    Parameters:
    - conn: The Swift connection object.
    - container_name: The name of the Swift container.
    - object_name: The name of the object to download.
    - local_path: The local path to save the downloaded file.

    WARNING: This function will overwrite the file at local_path if it already exists.

    Returns:
    None
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, 'wb') as f:
        f.write(conn.get_object(container_name, object_name)[1])