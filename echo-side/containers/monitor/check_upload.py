from datetime import datetime, timedelta
import sys
import os
import bucket_manager as bm
import pandas as pd


keys = bm.get_keys('S3')
s3_host = 'echo.stfc.ac.uk'
access_key = keys['access_key']
secret_key = keys['secret_key']
import warnings
warnings.filterwarnings('ignore')
s3 = bm.get_resource(access_key,secret_key,s3_host)

bucket_name = sys.argv[1]
bucket = s3.Bucket(bucket_name)

files_to_checksum = []

try:
    for ob in bucket.objects.all():
        # print(ob.key)
        if not ob.key.endswith('.symlink'):
            files_to_checksum.append(ob.key)
except Exception as e:
    if '(NoSuchBucket)' in str(e).split():
        print('NoSuchBucket')

upload_log_URI = sys.argv[2]
upload_log_path = os.path.join(os.getcwd(),'upload_log.csv')

s3.meta.client.download_file(bucket_name, upload_log_URI, 'upload_log.csv')

upload_log = pd.read_csv(upload_log_path)

print(upload_log)

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
