"""
DAG для ежедневной загрузки JSON данных из Auto.ru в ClickHouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException
import requests
import json
import re
import logging
from typing import Dict, Any, Optional

# Настройка логгера
logger = logging.getLogger(__name__)

# Конфигурация по умолчанию
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@example.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# Конфигурация DAG
dag = DAG(
    'clickhouse_json_loader',
    default_args=default_args,
    description='Ежедневная загрузка JSON данных из Auto.ru в ClickHouse',
    schedule_interval='0 13 * * *', 
    catchup=False,
    tags=['clickhouse', 'auto.ru', 'etl'],
    max_active_runs=1,
)

class ClickHouseJsonLoader:
    """Класс для загрузки JSON данных в ClickHouse"""
    
    def __init__(self, host: str = "ch1", port: int = 8123, 
                 user: str = "admin", password: str = "password",
                 database: str = "default"):
        
        # Получаем настройки из Airflow Variables или используем значения по умолчанию
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.base_url = f"http://{host}:{port}"

        # URL для парсинга (можно вынести в Variable)
        self.target_url = Variable.get(
            "AUTORU_TARGET_URL", 
            "https://auto.ru/cars/bmw/m3/23978803/new/?output_type=list"
        )
        
        logger.info(f"ClickHouse loader initialized for {self.host}:{self.port}")
    
    def test_connection(self, **context) -> bool:
        """Проверка подключения к ClickHouse"""
        task_instance = context.get('task_instance')
        
        try:
            response = requests.get(f"{self.base_url}/ping", timeout=10)
            
            if response.status_code == 200:
                logger.info(f"✅ Подключение к ClickHouse {self.host} успешно")
                
                # Сохраняем метаданные в XCom
                if task_instance:
                    task_instance.xcom_push(key='clickhouse_connection', value='success')
                
                return True
            else:
                logger.error(f"❌ Ошибка подключения к ClickHouse: {response.status_code}")
                raise AirflowException(f"ClickHouse недоступен: {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к ClickHouse {self.host}: {e}")
            raise AirflowException(f"Ошибка подключения к ClickHouse: {e}")
    
    def execute_query(self, query: str) -> Optional[Dict]:
        """Выполняет запрос к ClickHouse"""
        try:
            response = requests.post(
                self.base_url,
                params={'query': query},
                auth=(self.user, self.password),
                timeout=60
            )
            
            if response.status_code == 200:
                logger.debug(f"Запрос выполнен успешно")
                return {'success': True, 'data': response.text}
            else:
                logger.error(f"Ошибка запроса ({response.status_code}): {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка выполнения запроса: {e}")
            return None
    
    def fetch_json_from_url(self, **context) -> Optional[Dict]:
        """Получает JSON-LD данные с указанного URL"""
        task_instance = context.get('task_instance')
        url = self.target_url
        
        logger.info(f"🔍 Получаем JSON-LD с: {url}")
        
        try:
            # Получаем страницу
            response = requests.get(url, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"❌ Ошибка HTTP {response.status_code} для {url}")
                raise AirflowException(f"Ошибка получения страницы: {response.status_code}")
            
            logger.info(f"✅ Страница загружена ({len(response.text):,} байт)")
            
            # Ищем JSON-LD блоки
            pattern = r'<script\s+type="application/ld\+json">\s*({.*?})\s*</script>'
            matches = re.findall(pattern, response.text, re.DOTALL)
            
            if not matches:
                logger.warning(f"⚠️ JSON-LD данные не найдены на {url}")
                return None
            
            logger.info(f"✅ Найдено JSON-LD блоков: {len(matches)}")
            
            # Ищем блок с типом Product
            product_data = None
            for i, json_str in enumerate(matches, 1):
                try:
                    data = json.loads(json_str)
                    
                    if data.get('@type') == 'Product':
                        product_data = data
                        logger.info(f"🎯 Найден блок с @type='Product'")
                        break
                except json.JSONDecodeError as e:
                    logger.warning(f"⚠️ Ошибка декодирования JSON блока {i}: {e}")
                    continue
            
            # Если не нашли Product, берем первый валидный блок
            if not product_data and matches:
                try:
                    product_data = json.loads(matches[0])
                    logger.info(f"📄 Используем первый JSON-LD блок")
                except Exception as e:
                    logger.error(f"❌ Не удалось декодировать ни один JSON блок: {e}")
                    return None
            
            if product_data:
                name = product_data.get('name', 'Неизвестно')
                json_size = len(json.dumps(product_data))
                
                logger.info(f"📝 Название: {name}")
                logger.info(f"📏 Размер данных: {json_size:,} байт")
                
                # Сохраняем метаданные в XCom
                if task_instance:
                    task_instance.xcom_push(key='json_data_name', value=name)
                    task_instance.xcom_push(key='json_data_size', value=json_size)
                    task_instance.xcom_push(key='json_data_url', value=url)
                
                return product_data
            else:
                logger.warning("❌ Не удалось получить JSON данные")
                return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения JSON: {e}")
            raise AirflowException(f"Ошибка получения JSON: {e}")
    
    def insert_json_data(self, **context) -> bool:
        """Вставляет JSON данные в ClickHouse"""
        task_instance = context.get('task_instance')
        
        # Получаем данные из предыдущей задачи через XCom
        ti = context['ti']
        json_data = ti.xcom_pull(task_ids='fetch_json_data', key='return_value')
        
        if not json_data:
            logger.error("❌ Нет данных для вставки")
            raise AirflowException("Нет данных для вставки в ClickHouse")
        
        try:
            # Преобразуем JSON в строку
            json_str = json.dumps(json_data, ensure_ascii=False)
            
            # Экранируем одинарные кавычки для SQL
            json_str_escaped = json_str.replace("'", "''")
            
            # Формируем INSERT запрос
            query = f"""
            INSERT INTO {self.database}.car_raw_json_data 
            (source, query_dttm, raw_json)
            VALUES (
                'auto.ru',
                now(),
                '{json_str_escaped}'
            )
            """
            
            logger.info(f"📤 Записываем JSON данные в ClickHouse...")
            logger.info(f"📏 Размер JSON: {len(json_str):,} байт")
            
            result = self.execute_query(query)
            
            if result and result.get('success'):
                logger.info("✅ JSON успешно записан в ClickHouse")
                
                # Сохраняем метаданные в XCom
                if task_instance:
                    task_instance.xcom_push(key='insert_status', value='success')
                    task_instance.xcom_push(key='inserted_json_size', value=len(json_str))
                
                return True
            else:
                logger.error("❌ Не удалось записать данные в ClickHouse")
                raise AirflowException("Ошибка записи в ClickHouse")
                
        except Exception as e:
            logger.error(f"❌ Ошибка вставки данных: {e}")
            raise AirflowException(f"Ошибка вставки данных: {e}")
    
    def verify_insert(self, **context) -> None:
        """Проверяет успешность вставки данных"""
        task_instance = context.get('task_instance')
        
        try:
            # Проверяем последнюю запись
            query = f"""
            SELECT 
                count() as total_rows,
                max(query_dttm) as last_insert_time,
                length(raw_json) as last_json_size
            FROM {self.database}.car_raw_json_data 
            WHERE source = 'auto.ru'
            ORDER BY query_dttm DESC 
            LIMIT 1
            """
            
            result = self.execute_query(query)
            
            if result and result.get('success'):
                logger.info(f"📊 Проверка вставки:")
                logger.info(result['data'])
                
                # Сохраняем результат проверки
                if task_instance:
                    task_instance.xcom_push(key='verification_result', value=result['data'])
            else:
                logger.warning("⚠️ Не удалось проверить вставку данных")
                
        except Exception as e:
            logger.error(f"❌ Ошибка проверки данных: {e}")

def cleanup_old_data(**context):
    """Очистка старых данных (опционально)"""
    try:
        # Настройки из Variables
        retention_days = int(Variable.get("CLICKHOUSE_RETENTION_DAYS", "30"))
        
        ch = ClickHouseJsonLoader()
        
        # Удаляем данные старше retention_days дней
        delete_query = f"""
        ALTER TABLE {ch.database}.car_raw_json_data_local 
        DELETE WHERE query_dttm < now() - INTERVAL {retention_days} DAY
        """
        
        result = ch.execute_query(delete_query)
        
        if result and result.get('success'):
            logger.info(f"✅ Очищены данные старше {retention_days} дней")
        else:
            logger.warning(f"⚠️ Не удалось очистить старые данные")
            
    except Exception as e:
        logger.error(f"❌ Ошибка очистки данных: {e}")

def send_success_notification(**context):
    """Отправка уведомления об успешном выполнении"""
    try:
        ti = context['ti']
        
        # Получаем метаданные из XCom
        json_name = ti.xcom_pull(task_ids='fetch_json_data', key='json_data_name')
        json_size = ti.xcom_pull(task_ids='fetch_json_data', key='json_data_size')
        
        message = f"""
        ✅ DAG clickhouse_json_loader выполнен успешно!
        
        📊 Детали выполнения:
        - Название данных: {json_name or 'Неизвестно'}
        - Размер JSON: {json_size or 0:,} байт
        - Время выполнения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        
        logger.info(message)
        
        # Здесь можно добавить отправку email, slack и т.д.
        # Например:
        # send_email(to=default_args['email'], subject='DAG успешно выполнен', body=message)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления: {e}")

def send_failure_notification(context):
    """Отправка уведомления об ошибке"""
    try:
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        execution_date = context['execution_date']
        exception = context.get('exception', 'Неизвестная ошибка')
        
        message = f"""
        ❌ DAG {dag_id} завершился с ошибкой!
        
        Детали ошибки:
        - Задача: {task_id}
        - Время выполнения: {execution_date}
        - Ошибка: {exception}
        """
        
        logger.error(message)
        
        # Здесь можно добавить отправку email, slack и т.д.
        # Например:
        # send_email(to=default_args['email'], subject='Ошибка выполнения DAG', body=message)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления об ошибке: {e}")

# Инициализируем loader
loader = ClickHouseJsonLoader()

# Определяем задачи DAG

# Начало DAG
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Проверка подключения к ClickHouse
check_connection_task = PythonOperator(
    task_id='check_clickhouse_connection',
    python_callable=loader.test_connection,
    dag=dag,
)

# Получение JSON данных
fetch_json_task = PythonOperator(
    task_id='fetch_json_data',
    python_callable=loader.fetch_json_from_url,
    dag=dag,
)

# Вставка данных в ClickHouse
insert_data_task = PythonOperator(
    task_id='insert_json_data',
    python_callable=loader.insert_json_data,
    dag=dag,
)

# Проверка вставки
verify_insert_task = PythonOperator(
    task_id='verify_insert',
    python_callable=loader.verify_insert,
    dag=dag,
)

# Очистка старых данных (опционально)
cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag,
)

# Уведомление об успехе
success_notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
    trigger_rule='all_success',
)

# Конец DAG
end_task = DummyOperator(
    task_id='end',
    dag=dag,
    trigger_rule='all_done',
)

# Настройка зависимостей задач
start_task >> check_connection_task >> fetch_json_task >> insert_data_task
insert_data_task >> verify_insert_task >> cleanup_task
cleanup_task >> success_notification_task >> end_task

# Настраиваем обработку ошибок
dag.on_failure_callback = send_failure_notification
