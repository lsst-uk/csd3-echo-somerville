"""
Created on Tue Jun 14

@author: dmckay
"""
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

connection = BaseHook.get_connection('EchoS3')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
}

dag = DAG(
    's3_data_monitor',
    default_args=default_args,
    description='Monitor S3 bucket for new fits files and checksum them - uses containerise code on local registry',
    schedule=timedelta(days=1),
)

monitor = KubernetesPodOperator(
    namespace='airflow',
    image='localhost:32000/monitor:latest',
    name='monitor',
    task_id='monitor',
    dag=dag,
)

#graph
monitor
