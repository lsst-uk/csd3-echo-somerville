#!/usr/env/python

"""
Helper functions for using an s3 bucket
"""
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

#access_key = keys['access_key']
#secret_key = keys['secret_key']

def get_conn(access_key, secret_key, host):
	return boto.connect_s3(
		aws_access_key_id = access_key,
		aws_secret_access_key = secret_key,
		host = host,
		calling_format = boto.s3.connection.OrdinaryCallingFormat(),
	)
#print('Connected to echo.stfc.ac.uk')

#print_buckets(conn)
#mybucket_name = 'dm-test'
#if mybucket_name not in [bucket.name for bucket in conn.get_all_buckets()]:
	#mybucket = conn.create_bucket(mybucket_name)
	#print(f'Added bucket: {mybucket_name}')
#else:
        #print(f'Bucket exists: {mybucket_name}')

## test create existing bucket
##print_buckets(conn)
##if mybucket_name not in [bucket.name for bucket in conn.get_all_buckets()]:
	##mybucket = conn.create_bucket(mybucket_name)
	##print(f'Added bucket: {mybucket_name}')
##else:
	##print(f'Bucket exists: {mybucket_name}')



#if delete:
#	conn.delete_bucket(mybucket_name)
#	print_buckets(conn)


