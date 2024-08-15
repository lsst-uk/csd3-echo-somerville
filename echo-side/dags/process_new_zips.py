from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import timedelta, datetime

bucket_names = []

def dl_bucket_names(**kwargs):
    import json
    import requests
    url = kwargs['url']
    r = requests.get(url)
    buckets = json.loads(r.text)
    for bucket in buckets:
        bucket_names.append(bucket['name'])
    print(bucket_names)
    
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
}

# Instantiate the DAG
dag = DAG(
    'process_zips',
    default_args=default_args,
    description='Runs process_collated_zips.py',
    schedule_interval=timedelta(hours=1), # change to daily once in production
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

with dag:

    get_bucket_names = PythonOperator(
        task_id = 'get_bucket_names',
        python_callable = dl_bucket_names,
        op_kwargs={'url':'https://raw.githubusercontent.com/lsst-uk/csd3-echo-somerville/main/echo-side/bucket_names/bucket_names.json'},
    )

    if len(bucket_names) > 0:
        for i, bucket_name in enumerate(bucket_names):
            process_zips_task = KubernetesPodOperator(
                task_id=f'process_zips_{i}',
                image='ghcr.io/lsst-uk/csd3-echo-somerville:latest',
                cmds=['./entrypoint.sh'],
                arguments=['python', 'csd3-echo-somerville/scripts/process_collated_zips.py', '--bucket_name', bucket_name, '--extract', '--nprocs', '16'],
                env_vars={
                    'ECHO_S3_ACCESS_KEY': Variable.get("ECHO_S3_ACCESS_KEY"),
                    'ECHO_S3_SECRET_KEY': Variable.get("ECHO_S3_SECRET_KEY"),
                },
                get_logs=True,
            )
    else:
        print('No bucket names found.')
            