"""
Created on Tue Jun 14

@author: dmckay
"""
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import timedelta

new_keys = []
connection = S3Hook(aws_conn_id='EchoS3')
bucket_name = 'LSST-IR-FUSION-TESTSTRATEGY'

def run_on_new_file(**kwargs):
    s3_hook = S3Hook(aws_conn_id='EchoS3')
    bucket_name=bucket_name,
    bucket_key='/',
    wildcard_match_suffix='.csv',
    all_keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=bucket_key, delimiter='/', suffix=wildcard_match_suffix, apply_wildcard=True),
    for key in all_keys:
        if s3_hook.get_key(key).last_modified > kwargs['execution_date']:
            new_keys.append(key)
    for key in new_keys:
        print(f'New key: {key}')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
}

dag = DAG(
    f'monitor-{bucket_name}',
    default_args=default_args,
    description=f'Monitor {bucket_name} S3 bucket for new CSV-formatted upload log files.',
    schedule=timedelta(days=1),
)

s3_sensor = S3KeySensor(
    task_id='s3_sensor',
    bucket_name=bucket_name,
    bucket_key='*.csv',
    wildcard_match=True,
    aws_conn_id='EchoS3',
    timeout=1 * 60 * 60,
    poke_interval=60,
    dag=dag,
    default_args=default_args,
)

run_on_new_file_op = PythonOperator(
    task_id='run_on_new_file',
    python_callable=run_on_new_file,
    dag=dag,
    default_args=default_args,
    op_kwargs={'ds': '{{ ds }}'},
)

check_csv_ops = []
for key in new_keys:
    check_csv_op = KubernetesPodOperator(
        task_id=f"check_key_{key}",
        name=f"check-key-{key}",
        namespace="airflow",
        image="ghcr.io/lsst-uk/csd3-echo-somerville:latest",
        cmds=["python", "scripts/check_upload.py"],
        env_vars={'ECHO_S3_ACCESS_KEY': connection.access_key, 'ECHO_S3_SECRET_KEY': connection.secret_key},
        arguments=[bucket_name, key],
        get_logs=True,
        dag=dag,
    )
    check_csv_ops.append(check_csv_op)

#graph
s3_sensor >> run_on_new_file_op >> check_csv_ops
