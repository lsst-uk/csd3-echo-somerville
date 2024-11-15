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
    
def get_keys(api: str ='s3') -> None:
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
            access_key = os.environ['ECHO_S3_ACCESS_KEY']
            secret_key = os.environ['ECHO_S3_SECRET_KEY']
        except KeyError:
            raise KeyError('Set ECHO_S3_ACCESS_KEY and ECHO_S3_SECRET_KEY environment variables.')
        return {'access_key': access_key, 'secret_key': secret_key}
    elif api == 'swift':
        try:
            user = os.environ['ECHO_SWIFT_USER']
            secret_key = os.environ['ECHO_SWIFT_SECRET_KEY']
        except KeyError:
            raise KeyError('Set ECHO_SWIFT_USER and ECHO_SWIFT_SECRET_KEY environment variables.')
        return {'user': user, 'secret_key': secret_key}
    else:
        raise ValueError(f'Invalid API: {api}')

def get_resource(access_key: str, secret_key: str, s3_host: str):
    """
    Creates and returns an S3 resource object for the specified S3 endpoint.

    Parameters:
    - access_key: The access key for the S3 endpoint.
    - secret_key: The secret key for the S3 endpoint.
    - s3_host: The hostname of the S3 endpoint.

    Returns:
    An S3 resource object.
    """
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    return session.resource(
        service_name='s3',
        endpoint_url=s3_host',
        verify=False  # Disable SSL verification for non-AWS S3 endpoints
    )

def get_client(access_key: str, secret_key: str, s3_host:str):
    """
    Creates and returns an S3 client object for the specified S3 endpoint.

    Parameters:
    - access_key: The access key for the S3 endpoint.
    - secret_key: The secret key for the S3 endpoint.
    - s3_host: The hostname of the S3 endpoint.

    Returns:
    An S3 client object.
    """
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    return session.client(
        service_name='s3',
        endpoint_url=s3_host,
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

def object_list_swift(conn: swiftclient.Connection, container_name: str, prefix : str = '', full_listing: bool = True, count: bool = False) -> list[str]:
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

def get_conn_swift(user: str, access_key: str, host: str) -> swiftclient.Connection:
    """
    Creates and returns a Swift connection object for the specified Swift endpoint.

    Parameters:
    - user: The Swift user.
    - access_key: The Swift access key.
    - host: The Swift authentication URL.

    Returns:
    A Swift connection object.
    """
    return swiftclient.Connection(
        user=user,
        key=access_key,
        authurl=host
    )

def get_service_swift(user: str, secret_key: str, host: str) -> swiftclient.service.SwiftService:
    """
    Creates and returns a Swift service object for the specified Swift endpoint.

    Parameters:
    - user: The Swift user.
    - secret_key: The Swift secret key.
    - host: The Swift authentication URL.

    Returns:
    A Swift service object.
    """
    return swiftclient.service.SwiftService(
        {
            'auth_version': '1',
            'os_auth_url': host,
            'os_username': user.split(':')[1],
            'os_password': secret_key,
            'os_project_name': user.split(':')[0],
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