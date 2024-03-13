from datetime import datetime, timedelta
import sys
import os
import pandas as pd
import hashlib
from tqdm import tqdm

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import bucket_manager.bucket_manager as bm

keys = bm.get_keys('S3')
s3_host = 'echo.stfc.ac.uk'
access_key = keys['access_key']
secret_key = keys['secret_key']
import warnings
warnings.filterwarnings('ignore')
s3 = bm.get_resource(access_key,secret_key,s3_host)

bucket_name = sys.argv[1]
bucket = s3.Bucket(bucket_name)

upload_log_URI = sys.argv[2]
upload_log_path = os.path.join(os.getcwd(),'upload_log.csv')

try:
    s3.meta.client.download_file(bucket_name, upload_log_URI, 'upload_log.csv')
except Exception as e:
    if '(NoSuchBucket)' in str(e).split():
        print(f'NoSuchBucket {bucket_name}')
    elif '(NoSuchKey)' in str(e).split():
        print(f'NoSuchKey {upload_log_URI}')

upload_log = pd.read_csv(upload_log_path)[['FILE_SIZE', 'DESTINATION_KEY', 'CHECKSUM']]
upload_log = upload_log[upload_log['DESTINATION_KEY'].str.endswith('.symlink') == False]

# print(upload_log)

# Get objects "and checksum them"
new_checksum = [hashlib.md5(s3.Object(bucket_name, URI).get()['Body'].read()).hexdigest().encode('utf-8') for URI in tqdm(upload_log['DESTINATION_KEY'])]

# match booleans
checksums_match = None
sizes_match = None
# Compare checksums
upload_log['NEW_CHECKSUM'] = new_checksum
upload_log['CHECKSUM_MATCH'] = upload_log['CHECKSUM'] == upload_log['NEW_CHECKSUM']
if upload_log['CHECKSUM_MATCH'].all():
    checksums_match = True
    print('Checksums match.')
else:    
    checksums_match = False
    print('Checksums do not match.')
# Compare file sizes
upload_log['SIZE_ON_S3'] = [s3.Object(bucket_name, URI).content_length for URI in tqdm(upload_log['DESTINATION_KEY'])]
upload_log['SIZE_MATCH'] = upload_log['SIZE_ON_S3'] == upload_log['FILE_SIZE']
if upload_log['SIZE_MATCH'].all():
    sizes_match = True
    print('Sizes match.')
else:
    sizes_match = False
    print('Sizes do not match.')

if checksums_match and sizes_match:
    print('Upload successful.')
    # Clean up
    os.remove(upload_log_path)
    sys.exit(0)
else:
    print('Upload failed.')
    # Clean up
    os.remove(upload_log_path)
    sys.exit(1)
