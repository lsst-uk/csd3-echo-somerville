import airflow
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days = 1),
    'email': ['d.mckay@epcc.ed.ac.uk'],
    'email_on_failure': True,
    'email_on_retry': True,
}

dag = DAG(
    's3_data_monitor',
    default_args=default_args,
    description='Monitor S3 bucket for new fits files and checksum them',
    schedule='@daily',
)

def download_and_checksum_files():
    s3_conn = S3Hook(aws_conn_id='EchoS3')

    # Get the list of .fits files in the S3 bucket
    # Should prefix be suffix??
    fits_files = s3_conn.list_objects(bucket_name='dm-test',
                                     prefix='*.fits')['Contents']
    print(fits_files)
    file_list_object = s3_conn.get_object(bucket_name='dm-test',key='VIDEO-20180819-files.csv')
    file_list_data = file_list_object['Body'].read().decode('utf-8')
    print(file_list_data)

    #verified_files = []
    #for fits_file in fits_files:
        #file_key = fits_file['Key']
#
        ## Download the .fits file from S3
        #file_object = s3_conn.get_object(bucket_name='your-ceph-s3-bucket',
                                          #key=file_key)
        #file_data = file_object['Body'].read()
#
        ## Calculate the checksum of the .fits file
        #md5_checksum = hashlib.md5(file_data).hexdigest()
#
        ## Verify the checksum against the corresponding .md5 file
        #checksum_key = file_key.replace('.fits', '.md5')
        #checksum_object = s3_conn.get_object(bucket_name='your-ceph-s3-bucket',
                                              #key=checksum_key)
        #checksum_data = checksum_object['Body'].read().decode('utf-8')
#
        #if md5_checksum == checksum_data:
            #verified_files.append(file_key)

#operators
dl_chk = PythonOperator(
        task_id = 'download_and_checksum',
        python_callable = download_and_checksum_files,
        dag = dag,
        )

#graph
dl_chk
