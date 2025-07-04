from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

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
bucket_names = dl_bucket_names(
    'https://raw.githubusercontent.com/lsst-uk/csd3-echo-somerville/main/echo-side/bucket_names/bucket_names.json'
)


def print_bucket_name(bucket_name):
    print(bucket_name)


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
    start_date=datetime(2024, 1, 1, 12, 0, 0),  # daily at noon
    catchup=False,
) as dag:

    print_bucket_name_tasks = [
        PythonOperator(
            task_id=f'print_bucket_name_{bucket_name}',
            python_callable=print_bucket_name,
            op_kwargs={'bucket_name': bucket_name},
        ) for bucket_name in bucket_names]

    create_clean_up_zips_dask_tasks = [
        KubernetesPodOperator(
            task_id=f'clean_up_zips_{bucket_name}',
            name=f'clean_up_zips_{bucket_name}-Dask-pod',
            namespace='airflow',
            image='ghcr.io/lsst-uk/ces:latest',
            cmds=['/entrypoint.sh'],
            arguments=[
                'python',
                'csd3-echo-somerville/scripts/clean_up_zips.py',
                '-y',
                '--bucket-name',
                bucket_name,
                '-D',
                '-w',
                '8',
            ],
            env_vars={
                'S3_ACCESS_KEY': Variable.get("S3_ACCESS_KEY"),
                'S3_SECRET_KEY': Variable.get("S3_SECRET_KEY"),
                'S3_HOST_URL': Variable.get("S3_HOST_URL"),
                'ST_AUTH': Variable.get("ST_AUTH"),
                'ST_USER': Variable.get("ST_USER"),
                'ST_KEY': Variable.get("ST_KEY"),
            },
            do_xcom_push=True,
            is_delete_operator_pod=True,
            in_cluster=True,
            service_account_name='airflow-dask-executor',
            get_logs=True,
            dag=dag,
        ) for bucket_name in bucket_names]

    # Set task dependencies
    for print_bucket_name_task, create_clean_up_zips_dask_task in zip(
        print_bucket_name_tasks,
        create_clean_up_zips_dask_tasks
    ):
        print_bucket_name_task >> create_clean_up_zips_dask_task
