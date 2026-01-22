# dags/clickhouse_minimal_tables.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

def list_tables_simple():
    """Минимальный список таблиц"""
    
    # Выполняем запрос
    response = requests.post(
        "http://ch1:8123",
        params={
            'query': """
            SELECT 
                database,
                name as table_name,
                engine
            FROM system.tables 
            WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema' )
            ORDER BY database, table_name
            """,
            'default_format': 'JSON'
        },
        auth=('admin', 'password'),
        timeout=10
    )
    
    if response.status_code == 200:
        data = response.json()
        print("="*50)
        print("СПИСОК ТАБЛИЦ В CLICKHOUSE:")
        print("="*50)
        
        current_db = None
        
        print(f"\nВсего таблиц: {data.get('data', [])}")
    else:
        print(f"Ошибка: {response.status_code}")

with DAG(
    dag_id='minimal_tables_list',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['clickhouse'],
) as dag:
    
    task = PythonOperator(
        task_id='list_tables',
        python_callable=list_tables_simple,
    )