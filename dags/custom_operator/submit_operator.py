from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class SubmitOperator(BaseOperator):

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 spark_id,
                 spark_script,
                 *args, **kwargs):
        super(SubmitOperator, self).__init__(*args, **kwargs)
        self.spark_id = spark_id
        self.spark_script = spark_script

    def execute(self, context):
        pyspark_submit = SparkSubmitHook(conn_id = self.spark_id)
        exe_date = context.get('execution_date')
        exe_year = exe_date.year
        exe_month = exe_date.month
        exe_day = exe_date.day
        sub_command = self.spark_script + " {}/{}/{}".format(exe_date, exe_month, exe_day)
        pyspark_submit.submit(application = sub_command)