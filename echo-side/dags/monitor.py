from airflow import DAG
# from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'list_backup_csvs',
    default_args=default_args,
    description='List backup CSV files from S3 bucket',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

# BashOperator to run the script
list_csv_files = BashOperator(
    task_id='list_csv_files',
    bash_command='python csd3-echo-somerville/scripts/list_backup_csvs.py --bucket_name LSST-IR-FUSION-Butlers',
    env={
        'ECHO_S3_ACCESS_KEY': Variable.get("ECHO_S3_ACCESS_KEY"),
        'ECHO_S3_SECRET_KEY': Variable.get("ECHO_S3_SECRET_KEY"),
    },
    dag=dag,
)

# Set the task sequence
list_csv_files