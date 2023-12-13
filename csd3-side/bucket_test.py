import boto
import json
import os
import boto.s3.connection

with open(os.sep.join([os.environ['HOME'],'lsst_keys.json']), 'r') as keyfile:
	keys = json.load(keyfile)

access_key = keys['access_key']
secret_key = keys['secret_key']

conn = boto.connect_s3(
	aws_access_key_id = access_key,
	aws_secret_access_key = secret_key,
	host = 'echo.stfc.ac.uk',
	calling_format = boto.s3.connection.OrdinaryCallingFormat(),
	)

for bucket in conn.get_all_buckets():
	print("{name}\t{created}".format(name = bucket.name,created = bucket.creation_date))
