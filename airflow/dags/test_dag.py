from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='test_dag',
    schedule='@daily',  # Вместо schedule_interval
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:
    
    task1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )
    
    task2 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello from Airflow!"',
    )
    
    task1 >> task2