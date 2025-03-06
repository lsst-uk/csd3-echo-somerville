import re
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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

# for production
# bucket_names = dl_bucket_names('https://raw.githubusercontent.com/lsst-uk/csd3-echo-somerville/main/echo-side/bucket_names/bucket_names.json')
# for testing
bucket_names = ['LSST-IR-FUSION-test-zip-processing']

def print_bucket_name(bucket_name):
    print(bucket_name)

# Generate downstream tasks dynamically
def create_clean_up_zips_tasks(**kwargs):
    print(kwargs)
    # return ['']
    ti = kwargs['ti']
    tasks = []
    for bucket_name in bucket_names:
        prefixes = ti.xcom_pull(task_ids=f'get_prefixes_{bucket_name}')
        print(prefixes)
        for prefix in prefixes:
            task = KubernetesPodOperator(
                task_id=f'clean_up_zips_{prefix["bucket_name"]}_{prefix["prefix"]}',
                image='ghcr.io/lsst-uk/csd3-echo-somerville:latest',
                cmds=['/entrypoint.sh'],
                arguments=['python', 'csd3-echo-somerville/scripts/clean_up_zips.py', '-y', '-v', '--bucket-name', prefix["bucket_name"], '--extract', '--prefix', prefix["prefix"], '--nprocs', '6'],
                env_vars={
                    'S3_ACCESS_KEY': Variable.get("S3_ACCESS_KEY"),
                    'S3_SECRET_KEY': Variable.get("S3_SECRET_KEY"),
                    'S3_HOST_URL': Variable.get("S3_HOST_URL"),
                    'ST_AUTH': Variable.get("ST_AUTH"),
                    'ST_USER': Variable.get("ST_USER"),
                    'ST_KEY': Variable.get("ST_KEY"),
                },
                get_logs=True,
                dag=kwargs['dag'],
            )
            tasks.append(task)
    print([ task.task_id for task in tasks ])
    return tasks

def add_dynamic_tasks(**kwargs):
    tasks = create_clean_up_zips_tasks(**kwargs)
    for task in tasks:
        globals()[task.task_id] = task
        create_clean_up_zips_task >> task

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(seconds=60),
}

# Instantiate the DAG
with DAG(
        'clean_zips',
        default_args=default_args,
        description='Runs clean_up_zips.py',
        schedule=timedelta(days=1),
        start_date=datetime(2024, 1, 1, 12, 0, 0), # daily at noon
        catchup=False,
    ) as dag:

    print_bucket_name_task = [
        PythonOperator(
            task_id=f'print_bucket_name_{bucket_name}',
            python_callable=print_bucket_name,
            op_kwargs={'bucket_name': bucket_name},
        ) for bucket_name in bucket_names ]

    get_prefixes_task = [
        KubernetesPodOperator(
            task_id=f'get_prefixes_{bucket_name}',
            image='ghcr.io/lsst-uk/csd3-echo-somerville:latest',
            cmds=['/entrypoint.sh'],
            arguments=['python', 'csd3-echo-somerville/scripts/bucket_contents.py', '--bucket-name', bucket_name, '--names-to-json'],
            env_vars={
                'S3_ACCESS_KEY': Variable.get("S3_ACCESS_KEY"),
                'S3_SECRET_KEY': Variable.get("S3_SECRET_KEY"),
                'S3_HOST_URL': Variable.get("S3_HOST_URL"),
                'ST_AUTH': Variable.get("ST_AUTH"),
                'ST_USER': Variable.get("ST_USER"),
                'ST_KEY': Variable.get("ST_KEY"),
            },
            get_logs=True,
            do_xcom_push=True,
        ) for bucket_name in bucket_names ]

    create_clean_up_zips_task = PythonOperator(
        task_id='create_clean_up_zips_tasks',
        python_callable=add_dynamic_tasks,
        provide_context=True,
    )

     # Set task dependencies
    for task in print_bucket_name_task:
        task >> get_prefixes_task

    for task in get_prefixes_task:
        task >> create_clean_up_zips_task