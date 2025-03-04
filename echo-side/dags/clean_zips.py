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

bucket_names = dl_bucket_names('https://raw.githubusercontent.com/lsst-uk/csd3-echo-somerville/main/echo-side/bucket_names/bucket_names.json')

def print_bucket_name(bucket_name):
    print(bucket_name)

def process_prefixes(bucket_name, **kwargs):
    extn = re.compile(r'\.\w{3}$')
    ti = kwargs['ti']
    prefixes = ti.xcom_pull(task_ids=f'get_prefixes_{bucket_name}')
    prefixes_first_card = [ pre.split('/')[0] for pre in prefixes ]
    for prefix in prefixes_first_card:
        if extn.match(prefix):
            prefixes_first_card.remove(prefix) # first card is a file, not a prefix
    prefixes = list(set(prefixes_first_card))
    print(f'Prefixes for {bucket_name}: {prefixes}')
    return [{'bucket_name': bucket_name, 'prefix': prefix} for prefix in prefixes]

# Generate downstream tasks dynamically
def create_clean_up_zips_tasks(**kwargs):
    ti = kwargs['ti']
    tasks = []
    for bucket_name in bucket_names:
        prefixes = ti.xcom_pull(task_ids=f'process_prefixes_{bucket_name}')
        for prefix in prefixes:
            task_id = f'clean_up_zips_{prefix["bucket_name"]}_{prefix["prefix"]}'
            task = KubernetesPodOperator(
                task_id=task_id,
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
                dag=dag,
            )
            tasks.append(task)
    return tasks

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
        ) for bucket_name in bucket_names
    ]

    get_prefixes_task = [
        KubernetesPodOperator(
            task_id=f'get_prefixes_{bucket_name}',
            image='ghcr.io/lsst-uk/csd3-echo-somerville:latest',
            cmds=['/entrypoint.sh'],
            arguments=['python', 'csd3-echo-somerville/scripts/bucket_contents.py', '--bucket-name', bucket_name, '--names-only'],
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
        ) for bucket_name in bucket_names
    ]

    process_prefixes_task = [
        PythonOperator(
            task_id=f'process_prefixes_{bucket_name}',
            python_callable=process_prefixes,
            op_kwargs={'bucket_name': bucket_name},
            provide_context=True,
            do_xcom_push=True,
        ) for bucket_name in bucket_names
    ]

    create_clean_up_zips_task = PythonOperator(
        task_id='create_clean_up_zips_tasks',
        python_callable=create_clean_up_zips_tasks,
        provide_context=True,
    )

     # Set task dependencies
    for task in print_bucket_name_task:
        task >> get_prefixes_task

    for task in get_prefixes_task:
        task >> process_prefixes_task

    for task in process_prefixes_task:
        task >> create_clean_up_zips_task

    # # Add the dynamically created tasks to the DAG
    # clean_up_zips_tasks = create_clean_up_zips_tasks()
    # for task in clean_up_zips_tasks:
    #     create_clean_up_zips_task >> task