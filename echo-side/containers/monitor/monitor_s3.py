from datetime import datetime, timedelta
import sys
import boto
import boto.s3.connection
from bucket_manager import get_conn

access_key = sys.argv[1]
secret_key = sys.argv[2]

#def download_and_checksum_files():
    #s3_conn = S3Hook(aws_conn_id='EchoS3')
#
    ## Get the list of .fits files in the S3 bucket
    #fits_files = s3_conn.list_keys(bucket_name='dm-test',
                                     #prefix='*.fits')['Contents']
    #print(fits_files)
    #file_list_object = s3_conn.get_object(bucket_name='dm-test',key='VIDEO-20180819-files.csv')
    #file_list_data = file_list_object['Body'].read().decode('utf-8')
    #print(file_list_data)
#

s3_host = 'echo.stfc.ac.uk'
access_key = sys.argv[1]
secret_key = sys.argv[2]

conn = get_conn(access_key, secret_key, s3_host)

for bucket in conn.get_all_buckets():
        print("{name}\t{created}".format(
                name = bucket.name,
                created = bucket.creation_date,
        ))
