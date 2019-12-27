from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from submit_operator import SubmitOperator
from airflow.operators.python_operator import PythonOperator

conn_id = "emr_id"
conn = BaseHook.get_connection(conn_id)
user_name = conn.login
host = conn.host
key_file = conn.extra_dejson.get("key_file")
s3_bucket = 's3://joezcrmdb/'

# Construct command with placehlder to be inserted in SubmitOperator
ssh_command = 'ssh -i ' + key_file + ' ' + user_name + '@' + host + ' '
submit_command = '/usr/bin/spark-submit --master yarn ' + s3_bucket

# Set default argument for DAG
default_args = {
    'owner' : 'airflow',
    'start_date' : datetime(2019, 12, 27, 7),
    'depends_on_past': False,
    'retries' : 0,
    'retry_delay': timedelta(minutes=5),
}

# Create a dag to run daily
dag = DAG('spark_dag',
          default_args=default_args,
          description='Test spark submit with Airflow',
          schedule_interval = '@daily'
         )

# Task to process temperature data
command = ssh_command + '"' + submit_command + 'fact_temperature.py {}"'
process_temperature_task = SubmitOperator(
    task_id = 'process_temperature',
    dag = dag,
    bash_command = command)

# Task to check resulting temperature data
command = ssh_command + '"' + submit_command + 'check_temperature.py {}"'
check_temperature_task = SubmitOperator(
    task_id = 'check_temperature',
    dag = dag,
    bash_command = command)

# Task to process immigration data
command = ssh_command + '"' + submit_command + 'fact_immigration.py {}"'
process_immigration_task = SubmitOperator(
    task_id = 'process_immigration',
    dag = dag,
    bash_command = command)

# Task to check resulting immigration data
command = ssh_command + '"' + submit_command + 'check_immigration.py {}"'
check_immigration_task = SubmitOperator(
    task_id = 'check_immmigration',
    dag = dag,
    bash_command = command)


process_temperature_task >> check_temperature_task >> process_immigration_task
process_immigration_task >> check_immigration_task



