"""
DAG для тестирования подключения к ClickHouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import requests
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

dag = DAG(
    'test_clickhouse_connection',
    default_args=default_args,
    description='Тест подключения к ClickHouse',
    schedule_interval=None,
    catchup=False,
    tags=['test'],
)

def test_clickhouse_connection(**context):
    """Тестирует подключение к ClickHouse"""
    
    # Получаем настройки из переменных Airflow
    host = Variable.get("CLICKHOUSE_HOST", "host.docker.internal")
    port = Variable.get("CLICKHOUSE_PORT", "8123")
    user = Variable.get("CLICKHOUSE_USER", "admin")
    password = Variable.get("CLICKHOUSE_PASSWORD", "password")
    database = Variable.get("CLICKHOUSE_DATABASE", "default")
    
    base_url = f"http://{host}:{port}"
    
    logger.info("=" * 50)
    logger.info("🔍 ТЕСТ ПОДКЛЮЧЕНИЯ К CLICKHOUSE")
    logger.info("=" * 50)
    
    logger.info(f"Host: {host}")
    logger.info(f"Port: {port}")
    logger.info(f"User: {user}")
    logger.info(f"Database: {database}")
    logger.info(f"URL: {base_url}")
    
    # 1. Проверка ping
    logger.info("\n1. Проверяем ping...")
    try:
        response = requests.get(f"{base_url}/ping", timeout=10)
        if response.status_code == 200:
            logger.info(f"✅ Ping успешен: {response.text}")
        else:
            logger.error(f"❌ Ping не удался: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Ошибка ping: {e}")
        return False
    
    # 2. Проверка простого запроса
    logger.info("\n2. Проверяем простой запрос...")
    try:
        query = "SELECT 1 as test, version() as version"
        response = requests.post(
            base_url,
            params={'query': query},
            auth=(user, password),
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info(f"✅ Запрос выполнен: {response.text}")
        else:
            logger.error(f"❌ Ошибка запроса: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Ошибка выполнения запроса: {e}")
        return False
    
    # 3. Проверка таблицы
    logger.info("\n3. Проверяем таблицу car_raw_json_data...")
    try:
        query = """
        SELECT 
            database,
            name,
            engine,
            total_rows
        FROM system.tables 
        WHERE name LIKE '%car_raw_json_data%'
        """
        
        response = requests.post(
            base_url,
            params={'query': query},
            auth=(user, password),
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info(f"✅ Таблицы найдены: {response.text}")
        else:
            logger.warning(f"⚠️ Таблицы не найдены: {response.text}")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось проверить таблицы: {e}")
    
    # 4. Проверка данных в таблице
    logger.info("\n4. Проверяем данные в таблице...")
    try:
        query = """
        SELECT 
            count() as total_rows,
            min(query_dttm) as first_date,
            max(query_dttm) as last_date
        FROM car_raw_json_data
        """
        
        response = requests.post(
            base_url,
            params={'query': query},
            auth=(user, password),
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info(f"✅ Данные в таблице: {response.text}")
        else:
            logger.warning(f"⚠️ Нет данных в таблице: {response.text}")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось проверить данные: {e}")
    
    logger.info("\n" + "=" * 50)
    logger.info("✅ ТЕСТ ЗАВЕРШЕН УСПЕШНО")
    logger.info("=" * 50)
    
    return True

def test_network(**context):
    """Тестирование сети"""
    import socket
    import subprocess
    
    host = Variable.get("CLICKHOUSE_HOST", "host.docker.internal")
    port = int(Variable.get("CLICKHOUSE_PORT", "8123"))
    
    logger.info("\n🌐 ТЕСТ СЕТИ:")
    
    # Проверка DNS
    try:
        import socket
        ip = socket.gethostbyname(host)
        logger.info(f"✅ DNS разрешение: {host} → {ip}")
    except Exception as e:
        logger.error(f"❌ Ошибка DNS: {e}")
    
    # Проверка порта
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        if result == 0:
            logger.info(f"✅ Порт {port} на {host} открыт")
        else:
            logger.error(f"❌ Порт {port} на {host} закрыт")
        sock.close()
    except Exception as e:
        logger.error(f"❌ Ошибка проверки порта: {e}")

# Задачи DAG
start = DummyOperator(task_id='start', dag=dag)

test_network_task = PythonOperator(
    task_id='test_network',
    python_callable=test_network,
    dag=dag,
)

test_connection_task = PythonOperator(
    task_id='test_clickhouse_connection',
    python_callable=test_clickhouse_connection,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# Настройка зависимостей
start >> test_network_task >> test_connection_task >> end