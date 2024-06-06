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

def run_on_new_file(**kwargs):
    s3_hook = S3Hook(aws_conn_id='EchoS3')
    bucket_name='LSST-IR-Fusion-Butlers',
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
    'monitor-LSST-IR-Fusion-Butlers',
    default_args=default_args,
    description='Monitor LSST-IR-Fusion-Butlers S3 bucket for new CSV-formatted upload log files.',
    schedule=timedelta(days=1),
)

s3_sensor = S3KeySensor(
    task_id='s3_sensor',
    bucket_name='LSST-IR-Fusion-Butlers',
    bucket_key='*.csv',
    wildcard_match=True,
    aws_conn_id='EchoS3',
    timeout=1 * 60 * 60,
    poke_interval=60,
    dag=dag,
    default_args=default_args,
)

run_on_new_file = PythonOperator(
    task_id='run_on_new_file',
    python_callable=run_on_new_file,
    dag=dag,
    default_args=default_args,
    op_kwargs={'ds': '{{ ds }}'},
)

check_csv = KubernetesPodOperator(
    task_id="check_key",
    name="check-key",
    namespace="airflow",
    image="localhost:32000/check-csv:latest",
    cmds=["python", "-c"],
    arguments=[new_keys,connection.access_key,connection.secret_key],
    get_logs=True,
    dag=dag,
)

#graph
s3_sensor >> run_on_new_file >> check_csv
