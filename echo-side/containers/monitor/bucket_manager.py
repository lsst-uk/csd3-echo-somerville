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

def get_conn(access_key, secret_key, host):
	return boto.connect_s3(
		aws_access_key_id = access_key,
		aws_secret_access_key = secret_key,
		host = host,
		calling_format = boto.s3.connection.OrdinaryCallingFormat(),
	)
