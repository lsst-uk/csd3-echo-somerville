from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.models import Variable
from datetime import timedelta, datetime
from kubernetes.client import models as k8s
import os

# Create k8s storage mount 

logs_volume_mount = k8s.V1VolumeMount(name="logs-volume", mount_path="/lsst-backup-logs", sub_path=None, read_only=False,)
logs_volume = k8s.V1Volume(
    name="logs-volume",
    host_path=k8s.V1HostPathVolumeSource(path='/lsst-backup-logs', type="DirectoryOrCreate"),
)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 2,
}

new_csvs = []


def list_new_csvs(file_path):
    if os.path.exists(file_path):    
        with open(file_path, "r") as f:
            for line in f:
                new_csvs.append(line.strip())
        os.remove(file_path)

# Instantiate the DAG
dag = DAG(
    'list_backup_csvs',
    default_args=default_args,
    description='List backup CSV files from S3 bucket',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
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

list_new_csvs_op = PythonOperator(
    task_id='list_new_csvs_op',
    python_callable=list_new_csvs,
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image="ghcr.io/lsst-uk/csd3-echo-somerville:latest",
                        volume_mounts=[logs_volume_mount],
                    ),
                ],
                volumes=[logs_volume],
            )
        )
    },
    op_args=['/lsst-backup-logs/new_csv_files.txt'],
    dag=dag,
)

conditional_op = BranchPythonOperator(
    task_id='conditional_op',
    python_callable=lambda: 'check_new_csvs_subdag' if len(new_csvs) > 0 else 'end',
    dag=dag,
)

def subdag(parent_dag_name, child_dag_name, args, new_csvs):
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=args,
        # schedule_interval="@daily",
        # start_date=datetime(2024, 1, 1),
    )
    for csv in new_csvs:
        KubernetesPodOperator(
            task_id=f'check_{csv}',
            image='ghcr.io/lsst-uk/csd3-echo-somerville:latest',
            cmds=['./entrypoint.sh'],
            arguments=['python', 'csd3-echo-somerville/scripts/check_upload.py', 'LSST-IR-FUSION-Butlers', csv],
            dag=dag_subdag,
            volumes=[logs_volume],
            volume_mounts=[logs_volume_mount],
            get_logs=True,
        )
    return dag_subdag


check_new_csvs_subdag = SubDagOperator(
    task_id='check_new_csvs_subdag',
    subdag=subdag(dag.dag_id, 'check_new_csvs_subdag', default_args, new_csvs),
    # op_args=new_csvs,
    dag=dag,
)

end = PythonOperator(
    task_id='end',
    dag=dag,
    python_callable=lambda: print("No new CSV files to check."),
)

# Set the task sequence
list_csv_files >> compare_csv_file_lists >> list_new_csvs_op >> conditional_op >> [check_new_csvs_subdag,end]
        