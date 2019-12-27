from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

conn_id = "emr_id"

command = "/usr/bin/spark-submit --master yarn s3://joezcrmdb/"

# Set default argument for DAG
default_args = {
    'owner' : 'airflow',
    'start_date' : datetime(2019, 12, 25, 7),
    'depends_on_past': False,
    'retries' : 3,
    'retry_delay': timedelta(minutes=5),
}

# Create a dag to run daily
dag = DAG('spark_dag',
          default_args=default_args,
          description='Test spark submit with Airflow',
          schedule_interval = '@daily'
         )

sub_folder = ['',]

def get_date(sub_folder, *args, **kwargs):
    exe_date = kwargs['execution_date']
    sub_folder[0] = " {}/{}/{}".format(exe_date.year,
                                   exe_date.month,
                                   exe_date.day)
    
get_date_task = PythonOperator(
    task_id = "get_date",
    python_callable = get_date,
    dag = dag,
    provide_context = True,
    op_kwargs = {'sub_folder': sub_folder})

# Task to process temperature data
process_temperature_task = SSHOperator(
    task_id = 'process_temperature',
    dag = dag,
    ssh_conn_id = conn_id,
    command = command + "fact_temperature.py" + sub_folder[0])

# Task to check resulting temperature data
check_temperature_task = SSHOperator(
    task_id = 'check_temperature',
    dag = dag,
    ssh_conn_id = conn_id,
    command = command + "check_temperature.py" + sub_folder[0])

# Task to process immigration data
process_immigration_task = SSHOperator(
    task_id = 'process_immigration',
    dag = dag,
    ssh_conn_id = conn_id,
    command = command + "fact_immigration.py" + sub_folder[0])

# Task to check resulting immigration data 
check_immigration_task = SSHOperator(
    task_id = 'check_immmigration',
    dag = dag,
    ssh_conn_id = conn_id,
    command = command + "check_immigration.py" + sub_folder[0])



get_date_task >> process_temperature_task
process_temperature_task >> check_temperature_task >> process_immigration_task
process_immigration_task >> check_immigration_task



