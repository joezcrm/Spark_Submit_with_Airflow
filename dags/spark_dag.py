from datetime import datetime, timedelta
from airflow import DAG
from custom_operator.submit_operator import SubmitOperator

conn_id = "emr_id"

folder = "s3://joezcrmdb/"

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

# Task to process temperature data
process_temperature_task = SubmitOperator(
    task_id = 'process_temperature',
    dag = dag,
    spark_id = conn_id,
    spark_script = folder + "fact_temperature.py")

# Task to check resulting temperature data
check_temperature_task = SubmitOperator(
    task_id = 'check_temperature',
    dag = dag,
    spark_id = conn_id,
    spark_script = folder + "check_temperature.py")

# Task to process immigration data
process_immigration_task = SubmitOperator(
    task_id = 'process_immigration',
    dag = dag,
    spark_id = conn_id,
    spark_script = folder + "fact_immigration.py")

# Task to check resulting immigration data 
check_immigration_task = SubmitOperator(
    task_id = 'check_immmigration',
    dag = dag,
    spark_id = conn_id,
    spark_script = folder + "check_immigration.py")



process_temperature_task >> check_temperature_task >> process_immigration_task
process_immigration_task >> check_immigration_task



