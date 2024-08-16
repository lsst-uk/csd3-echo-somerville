from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import timedelta, datetime
from kubernetes.client import models as k8s
from airflow.models.baseoperator import chain

# Create k8s storage mount 

logs_volume_mount = k8s.V1VolumeMount(name="logs-volume", mount_path="/lsst-backup-logs", sub_path=None, read_only=False,)
logs_volume = k8s.V1Volume(
    name="logs-volume",
    host_path=k8s.V1HostPathVolumeSource(path='/lsst-backup-logs', type="DirectoryOrCreate"),
)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

# dict to hold new CSV files for multiple bucket names
new_csvs = {}

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

# Instantiate the DAG
with DAG(
    'monitor',
    default_args=default_args,
    description='List and compare backup CSV files from S3 bucket and trigger backup verifications if required.',
    schedule_interval=timedelta(days=1), # change to days=1 when in production
    start_date=datetime(2024, 1, 1, 12, 0, 0), # set to middle of the day to avoid issues with daylight savings and/or date stamps in filenames
    catchup=False,
) as dag:

    list_csv_files = [ KubernetesPodOperator(
        task_id=f'list_csv_files-{bucket_name}',
        image='ghcr.io/lsst-uk/csd3-echo-somerville:latest',
        cmds=['./entrypoint.sh'],
        arguments=['python', 'csd3-echo-somerville/scripts/list_backup_csvs.py', 
                   '--bucket_name', bucket_name, 
                   '--all-csvs', 
                   '--save-list', ''.join([f'/lsst-backup-logs/all-backup-logs-{bucket_name}','{{ ds_nodash }}','.txt']),
                   '--limit', '100000000'], # set to 100000000 to list all objects in the bucket - if a bucket ever exceeds 100M objects, this will need to be increased
        env_vars={
            'ECHO_S3_ACCESS_KEY': Variable.get("ECHO_S3_ACCESS_KEY"),
            'ECHO_S3_SECRET_KEY': Variable.get("ECHO_S3_SECRET_KEY"),
        },
        volumes=[logs_volume],
        volume_mounts=[logs_volume_mount],
        get_logs=True,
    ) for bucket_name in bucket_names]

    compare_csv_file_lists = [ KubernetesPodOperator(
        task_id=f'compare_csv_file_lists-{bucket_name}',
        image='ghcr.io/lsst-uk/csd3-echo-somerville:latest',
        cmds=['./entrypoint.sh'],
        arguments=['python', 'csd3-echo-somerville/scripts/compare_csv_file_lists.py', 
                   '--from-file', ''.join([f'/lsst-backup-logs/all-backup-logs-{bucket_name}','{{ ds_nodash }}','.txt']), 
                   '--to-file', ''.join([f'/lsst-backup-logs/new-csv-files-{bucket_name}','{{ ds_nodash }}','.txt'])],
        env_vars={
            'ECHO_S3_ACCESS_KEY': Variable.get("ECHO_S3_ACCESS_KEY"),
            'ECHO_S3_SECRET_KEY': Variable.get("ECHO_S3_SECRET_KEY"),
        },
        volumes=[logs_volume],
        volume_mounts=[logs_volume_mount],
        get_logs=True,
    ) for bucket_name in bucket_names]


    ### TODO
    ## Add a DAG trigger to run check_uploads_dag.py

    # Set the task sequence
    chain(
        list_csv_files,
        compare_csv_file_lists,
    )
            