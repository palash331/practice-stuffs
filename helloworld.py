from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hello():
	return 'Hello World!'

dag = DAG('hello_world', start_date=datetime(2019,2,7), schedule_interval='0 12 * * *', description='Simple Tutorial DAG', catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task',retries=3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task',python_callable=print_hello, dag=dag)

hello_operator.set_upstream(dummy_operator)

