from airflow.plugins_manager import AirflowPlugin
import operators

class CapstonePlugin(AirflowPlugin):
    name = "captone_plugin"
    operators = [
        operators.SubmitOperator
        ]
