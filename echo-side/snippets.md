# Code ideas
These code snippets are untested and may not work have completely unexpected effects. They are just gathered ideas.

## Ideas for monitor DAG
```python
import airflow
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

dag = DAG(
    's3_data_monitor',
    default_args=default_args,
    description='Monitor S3 bucket for new fits files, checksum them, and verify them in memory',
    schedule_interval='@daily',
)

def download_and_checksum_files():
    s3_conn = S3Hook()

    # Get the list of .fits files in the S3 bucket
    fits_files = s3_conn.list_objects(bucket_name='your-ceph-s3-bucket',
                                     prefix='*.fits')['Contents']

    verified_files = []
    for fits_file in fits_files:
        file_key = fits_file['Key']

        # Download the .fits file from S3
        file_object = s3_conn.get_object(bucket_name='your-ceph-s3-bucket',
                                          key=file_key)
        file_data = file_object['Body'].read()

        # Calculate the checksum of the .fits file directly in memory
        md5_checksum = hashlib.md5(file_data).hexdigest()

        # Verify the checksum against the corresponding .md5 file
        checksum_key = file_key.replace('.fits', '.md5')
        checksum_object = s3_conn.get_object(bucket_name='your-ceph-s3-bucket',
                                              key=checksum_key)
        checksum_data = checksum_object['Body'].read().decode('utf-8')

        if md5_checksum == checksum_data:
            verified_files.append(file_key)
```
