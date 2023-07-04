import os
import pendulum
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from operators.upload_files_to_s3_operator import UploadFilesToS3Operator
from operators.delete_files_operator import DeleteFilesOperator


default_args = {
        'owner': 'sejeong-park',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

with DAG(
        dag_id='delete_log_dag',
        start_date=datetime(2023, 7, 1) ,
        schedule_interval=timedelta(days=1), # test
        catchup=True,
        default_args=default_args
) as dag:
    
    # /home/ubuntu/kpop_dashboard/logs
    upload_airflow_logs = UploadFilesToS3Operator(
        task_id='upload_airflow_logs',
        conn_id='aws_conn_id',
        file_directory = '/opt/airflow/logs',
        file_pattern='*.log',
        bucket_name='kpop-analysis',
        folder_name='airflow_logs/'
    )
    
    cleanup_logs = DeleteFilesOperator(
        task_id = 'cleanup_logs',
        file_directory = '/opt/airflow/logs',
        file_pattern = '*.log'       
    )
    
    upload_airflow_logs >> cleanup_logs