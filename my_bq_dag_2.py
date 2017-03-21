"""
### My first dag to play around with bigquery and gcp stuff.
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 3, 10),    
    'email': ['xxx@xxx.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('my_bq_dag_2', schedule_interval='@once',default_args=default_args)

templated_command = """
    select '{{ dag_run.conf["lob"] }}' as lob, concat(string(current_timestamp()),' - Hello - {{ ds }}') as msg
"""    
     
bq_msg_1 = BigQueryOperator(
    dag = dag,
    task_id='my_bq_task_1',
    bql=templated_command,
    destination_dataset_table='airflow.test1',
    write_disposition='WRITE_APPEND',
    bigquery_conn_id='gcp_smoke'
)
