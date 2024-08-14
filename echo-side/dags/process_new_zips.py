from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import timedelta, datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 2,
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

get_bucket_names = BashOperator()

process_zips = KubernetesPodOperator(
    task_id='process_zips',
    image='ghcr.io/lsst-uk/csd3-echo-somerville:latest',
    cmds=['./entrypoint.sh'],
    arguments=['python', 'csd3-echo-somerville/scripts/process_collated_zips.py', '--bucket_name', '{{ bucket_name }}', '--extract', '--nprocs', '16'],
    env_vars={
        'ECHO_S3_ACCESS_KEY': Variable.get("ECHO_S3_ACCESS_KEY"),
        'ECHO_S3_SECRET_KEY': Variable.get("ECHO_S3_SECRET_KEY"),
    },
    dag=dag,
    get_logs=True,
)

# Set the task sequence
get_bucket_names >> process_zips
        