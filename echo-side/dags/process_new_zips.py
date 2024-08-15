from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models.xcom_arg import XComArg

from datetime import timedelta, datetime


def dl_bucket_names(url):
    import json
    import requests
    bucket_names = []
    r = requests.get(url)
    buckets = json.loads(r.text)
    for bucket in buckets:
        bucket_names.append(bucket['name'])
    print(f'Bucket names found: {bucket_names}')
    return bucket_names

bucket_names = dl_bucket_names('https://raw.githubusercontent.com/lsst-uk/csd3-echo-somerville/main/echo-side/bucket_names/bucket_names.json')

def print_bucket_name(bucket_name):
    print(bucket_name)
    
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
}

# Instantiate the DAG


with DAG(
        'process_zips',
        default_args=default_args,
        description='Runs process_collated_zips.py',
        schedule_interval=timedelta(hours=1), # change to daily once in production
        start_date=datetime(2024, 1, 1),
        catchup=False,
    ) as dag:

    print_bucket_name_task = [
        PythonOperator(
            task_id=f'print_bucket_name_{bucket_name}',
            python_callable=print_bucket_name,
            op_kwargs={'bucket_name': bucket_name},
        ) for bucket_name in bucket_names]

    # if len(bucket_names) > 0:
    #     print(f'Bucket names found: {bucket_names}')
    process_zips_task = [
        KubernetesPodOperator(
        task_id=f'process_zips_{bucket_name}',
        image='ghcr.io/lsst-uk/csd3-echo-somerville:latest',
        cmds=['./entrypoint.sh'],
        arguments=['python', 'csd3-echo-somerville/scripts/process_collated_zips.py', '--bucket_name', bucket_name, '--extract', '--nprocs', '16'],
        env_vars={
            'ECHO_S3_ACCESS_KEY': Variable.get("ECHO_S3_ACCESS_KEY"),
            'ECHO_S3_SECRET_KEY': Variable.get("ECHO_S3_SECRET_KEY"),
        },
        get_logs=True,
    ) for bucket_name in bucket_names]
    # else:
    #     print('No bucket names found.')

    print_bucket_name_task
    process_zips_task
            