#!/usr/env/python

"""
Helper functions for using an s3 bucket
"""
import boto3
import os
from botocore.exceptions import ClientError
import swiftclient

def print_buckets(resource):
    """
    Prints the names of all buckets in the S3 endpoint.

    Parameters:
    - resource: The S3 resource object.

    Returns:
    None
    """
    for b in resource.buckets.all():
        print(b.name)
    
def get_keys(api='S3'):
    """
    Retrieves the access key and secret key for the specified API.

    Parameters:
    - api: The API to retrieve the keys for. Can be 'S3' or 'Swift'.

    Returns:
    For S3 API: a dictionary containing the access key and secret key.
    For Swift API: a dictionary containing the user and secret key.
    """
    if api == 'S3':
        try:
            access_key = os.environ['ECHO_S3_ACCESS_KEY']
            secret_key = os.environ['ECHO_S3_SECRET_KEY']
        except KeyError:
            raise KeyError('Set ECHO_S3_ACCESS_KEY and ECHO_S3_SECRET_KEY environment variables.')
        return {'access_key': access_key, 'secret_key': secret_key}
    elif api == 'Swift':
        try:
            user = os.environ['ECHO_SWIFT_USER']
            secret_key = os.environ['ECHO_SWIFT_SECRET_KEY']
        except KeyError:
            raise KeyError('Set ECHO_SWIFT_USER and ECHO_SWIFT_SECRET_KEY environment variables.')
        return {'user': user, 'secret_key': secret_key}
    else:
        raise ValueError(f'Invalid API: {api}')

def get_resource(access_key, secret_key, s3_host):
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
        endpoint_url=f'https://{s3_host}',
        verify=False  # Disable SSL verification for non-AWS S3 endpoints
    )

def get_client(access_key, secret_key, s3_host):
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
        endpoint_url=f'https://{s3_host}',
        verify=False  # Disable SSL verification for non-AWS S3 endpoints
    )

def bucket_list(resource):
    """
    Returns a list of bucket names in the S3 endpoint.

    Parameters:
    - resource: The S3 resource object.

    Returns:
    A list of bucket names.
    """
    return [ b.name for b in resource.buckets.all() ]

def create_bucket(resource, bucket_name):
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

def print_objects(bucket):
    """
    Prints the keys of all objects in the specified bucket.

    Parameters:
    - bucket: The S3 bucket object.

    Returns:
    None
    """
    for obj in bucket.objects.all():
        print(obj.key)

def object_list(bucket):
    """
    Returns a list of keys of all objects in the specified bucket.

    Parameters:
    - bucket: The S3 bucket object.

    Returns:
    A list of object keys.
    """
    return [ obj.key for obj in bucket.objects.all() ]


def print_containers_swift(conn):
    """
    Prints the names of all containers in the Swift endpoint.

    Parameters:
    - conn: The Swift connection object.

    Returns:
    None
    """
    for container in conn.get_account()[1]:
        print(container['name'])

def print_contents_swift(conn,container_name):
    """
    Prints the names, sizes, and last modified timestamps of all objects in the specified container.

    Parameters:
    - container_name: The name of the Swift container.

    Returns:
    None
    """
    for data in conn.get_container(container_name)[1]:
        print('{0}\t{1}\t{2}'.format(data['name'], data['bytes'], data['last_modified']))

def get_conn_swift(user, access_key, host):
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
