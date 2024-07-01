from airflow import DAG
# from airflow.operators.docker_operator import DockerOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator

from kubernetes.client import models
from datetime import datetime
import os

# Create k8s storage mount 

logs_volume_mount = models.V1VolumeMount(name="logs-volume", mount_path="/lsst-backup-logs", sub_path=None, read_only=False,)
logs_volume = models.V1Volume(
    name="logs-volume",
    host_path=models.V1HostPathVolumeSource(path='/lsst-backup-logs', type="DirectoryOrCreate"),
)

# Define the function to compare the CSV file lists
def compare_csv_file_lists(log_folder):
    csv_files = []
    for file in os.listdir(log_folder):
        if file.startswith("lsst-backup-logs-") and file.endswith(".csv") and file.__contains__('{{ ds_nodash }}'):
            csv_files.append(file)
    for csv_file in csv_files:
        csv_file.replace("lsst-backup-logs-" + '{{ ds_nodash }}', "")
        csv_file.replace(".csv", "")
    csv_files = csv_files.sort()[-2:]

    print(csv_files)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'list_backup_csvs',
    default_args=default_args,
    description='List backup CSV files from S3 bucket',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
)

# KubernetesPodOperator to run the script
list_csv_files = KubernetesPodOperator(
    task_id='list_csv_files',
    image='ghcr.io/lsst-uk/csd3-echo-somerville:latest',
    cmds=['./entrypoint.sh'],
    arguments=['python', 'csd3-echo-somerville/scripts/list_backup_csvs.py', '--bucket_name', 'LSST-IR-FUSION-Butlers', '--save-list', ''.join(['/lsst-backup-logs/lsst-backup-logs-','{{ ts_nodash }}','.csv'])],
    env_vars={
        'ECHO_S3_ACCESS_KEY': Variable.get("ECHO_S3_ACCESS_KEY"),
        'ECHO_S3_SECRET_KEY': Variable.get("ECHO_S3_SECRET_KEY"),
    },
    dag=dag,
    volumes=[logs_volume],
    volume_mounts=[logs_volume_mount],
    get_logs=True,
)

compare_csv_file_lists = PythonOperator(
    task_id='compare_csv_file_lists',
    python_callable=compare_csv_file_lists,
    dag=dag,
)

# Set the task sequence
list_csv_files > compare_csv_file_lists