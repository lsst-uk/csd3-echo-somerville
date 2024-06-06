"""
Created on 24 / 05 / 24

@author: dmckay
"""
###
# Script to monitor the LSST-IR-FUSION-TESTCOLLATE S3 bucket for new ZIP files and extract them to new object keys.
###

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import timedelta
import zipfile

new_keys = []
connection = S3Hook(aws_conn_id='EchoS3')
extract_bools = []
extract_files_bools = []

def run_on_new_zipfiles(**kwargs):
    s3_hook = S3Hook(aws_conn_id='EchoS3')
    bucket_name='LSST-IR-FUSION-TESTCOLLATE',
    bucket_key='/',
    wildcard_match_suffix='.zip',
    all_keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=bucket_key, delimiter='/', suffix=wildcard_match_suffix, apply_wildcard=True),
    for key in all_keys:
        if s3_hook.get_key(key).last_modified > kwargs['execution_date']:
            new_keys.append(key)
    for key in new_keys:
        print(f'New key: {key}')
        extract_bools.append(False)

def check_zip(index, **kwargs):
    s3_hook = S3Hook(aws_conn_id='EchoS3')
    bucket_name='LSST-IR-FUSION-TESTCOLLATE',
    bucket_key='/',
    new_key = new_keys[index]
    extract_file = []
    with s3_hook.get_key(new_key) as zipfile:
        with zipfile.open() as file:
            with zipfile.ZipFile(file) as z:
                for name in z.namelist():
                    print(f'Found {name} in {new_key}')
                    if name in s3_hook.list_keys(bucket_name=bucket_name, prefix=bucket_key, delimiter='/'):
                        print(f'{name} already exists in {bucket_name}')
                        extract_file.append(False)
                    else:
                        print(f'{name} does not exist in {bucket_name}')
                        extract_file.append(True)
                extract_files_bools.append(extract_file)
                if not all(extract_file):
                    extract_bools[index] = False
                else:
                    extract_bools[index] = True
                    print(f'One or more files in {new_key} are new.')

def extract_zips(index, **kwargs):
    if extract_bools[index]:
        s3_hook = S3Hook(aws_conn_id='EchoS3')
        bucket_name='LSST-IR-FUSION-TESTCOLLATE',
        bucket_key='/',
        new_key = new_keys[index]
        with s3_hook.get_key(new_key) as zipfile:
            with zipfile.open() as zfile:
                with zipfile.ZipFile(zfile) as z:
                    for i, name in enumerate(z.namelist()):
                        if extract_files_bools[index][i]:
                            response = s3_hook._upload_file_obj(z.open(name), name, bucket_name=bucket_name)
                            print(f'Uploaded {name} to {bucket_name} - {response}')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
}

dag = DAG(
    'monitor-LSST-IR-FUSION-TESTCOLLATE',
    default_args=default_args,
    description='Monitor LSST-IR-FUSION-TESTCOLLATE S3 bucket for new ZIP files, check if contents exists, extract and upload if not.',
    schedule=timedelta(days=1),
)

s3_sensor = S3KeySensor(
    task_id='s3_sensor',
    bucket_name='LSST-IR-FUSION-TESTCOLLATE',
    bucket_key='/',
    wildcard_match=True,
    wildcard_match_suffix='.zip',
    aws_conn_id='EchoS3',
    timeout=1 * 60 * 60,
    poke_interval=60,
    dag=dag,
    default_args=default_args,
)

run_on_new_zipfiles_operator = PythonOperator(
    task_id='run_on_new_zipfiles',
    python_callable=run_on_new_zipfiles,
    dag=dag,
    default_args=default_args,
    op_kwargs={'ds': '{{ ds }}'},
)

def check_zip_operator(index):
    return PythonOperator(
        task_id='check_zip',
        python_callable=check_zip,
        dag=dag,
        args=index,
        default_args=default_args,
        op_kwargs={'ds': '{{ ds }}'},
)

def extract_zips_operator(index):
    return PythonOperator(
        task_id='check_zip',
        python_callable=check_zip,
        dag=dag,
        args=index,
        default_args=default_args,
        op_kwargs={'ds': '{{ ds }}'},
)

#graph
for i in range(len(new_keys)):
    s3_sensor >> run_on_new_zipfiles_operator >> check_zip_operator(i) >> extract_zips_operator(i)
# s3_sensor >> run_on_new_zipfiles_operator >> check_zip >> extract_zips
