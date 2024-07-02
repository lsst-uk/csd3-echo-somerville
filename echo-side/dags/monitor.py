from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.models import Variable
from datetime import timedelta
from kubernetes.client import models

# Create k8s storage mount 

logs_volume_mount = models.V1VolumeMount(name="logs-volume", mount_path="/lsst-backup-logs", sub_path=None, read_only=False,)
logs_volume = models.V1Volume(
    name="logs-volume",
    host_path=models.V1HostPathVolumeSource(path='/lsst-backup-logs', type="DirectoryOrCreate"),
)

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
    catchup=False,
)

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

compare_csv_file_lists = KubernetesPodOperator(
    task_id='compare_csv_file_lists',
    image='ghcr.io/lsst-uk/csd3-echo-somerville:latest',
    cmds=['./entrypoint.sh'],
    arguments=['python', 'csd3-echo-somerville/scripts/compare_csv_file_lists.py', '--path', '/lsst-backup-logs', '--datestamp', '{{ ds_nodash }}'],
    dag=dag,
    volumes=[logs_volume],
    volume_mounts=[logs_volume_mount],
    get_logs=True,
)

# Set the task sequence
list_csv_files >> compare_csv_file_lists