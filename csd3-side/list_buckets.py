import boto
import json
import os
import boto.s3.connection

def print_buckets(conn):
    for bucket in conn.get_all_buckets():
            print("{name}\t{created}".format(name = bucket.name,created = bucket.creation_date))
def get_keys(json_file):
    with open(json_file, 'r') as keyfile:
        keys = json.load(keyfile)
    return keys

def get_conn(access_key, secret_key, host):
    return boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = host,
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )


s3_host = 'echo.stfc.ac.uk'
keys = get_keys(os.sep.join([os.environ['HOME'],'lsst_keys.json']))

access_key = keys['access_key']
secret_key = keys['secret_key']

conn = get_conn(access_key, secret_key, s3_host)
for bucket in conn.get_all_buckets():
        print("{name}\t{created}".format(
                name = bucket.name,
                created = bucket.creation_date,
        ))
