from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import SubmitOperator

conn_id = "emr_id"

# Set default argument for DAG
default_args = {
    'owner' : 'airflow',
    'start_date' : datetime(2019, 12, 25, 7),
    'depends_on_past': False,
    'retries' : 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('spark_dag',
          default_args=default_args,
          description='Test spark submit with Airflow',
          schedule_interval = '@daily'
         )

process_temperature_task = SubmitOperator(
    task_id = 'process_temperature',
    dag = dag,
    spark_id = conn_id,
    spark_script = "airflow/fact_temperature.py")

check_temperature_task = SubmitOperator(
    task_id = 'check_temperature',
    dag = dag,
    spark_id = conn_id,
    spark_script = "airflow/check_temperature.py")

process_immigration_task = SubmitOperator(
    task_id = 'process_immigration',
    dag = dag,
    spark_id = conn_id,
    spark_script = "airflow/fact_immigration.py")
    
check_immigration_task = SubmitOperator(
    task_id = 'check_immmigration',
    dag = dag,
    spark_id = conn_id,
    spark_script = "airflow/check_immigration.py")



process_temperature_task >> check_temperature_task >> process_immigration_task
process_immigration_task >> check_immigration_task



