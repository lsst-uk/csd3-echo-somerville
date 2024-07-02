from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
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
    schedule_interval=timedelta(days=2),
    catchup=False,
)

clean_up_logs = KubernetesPodOperator(
    task_id='clean_up_logs',
    image='ghcr.io/lsst-uk/csd3-echo-somerville:latest',
    arguments=['csd3-echo-somerville/scripts/clean_up_logs.sh', '/lsst-backup-logs'],
    dag=dag,
    volumes=[logs_volume],
    volume_mounts=[logs_volume_mount],
    get_logs=True,
)

# Set the task sequence
clean_up_logs