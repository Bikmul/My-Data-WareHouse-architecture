"""
Динамический DAG для загрузки JSON данных из Auto.ru в ClickHouse
Генерирует отдельные задачи для каждой ссылки
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
import requests
import json
import re
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from urllib.parse import urlparse

# Настройка логгера
logger = logging.getLogger(__name__)

# ============================================
# КОНФИГУРАЦИЯ ССЫЛОК
# ============================================
# Список URL для парсинга
# Формат: марка/модель/кузов/состояние
AUTORU_URLS = [
    "https://auto.ru/cars/bmw/m3/23978803/new/?output_type=list",
    "https://auto.ru/cars/bmw/m5/23989035/new/?output_type=list",
    # Добавьте свои ссылки здесь
]

# Альтернативно можно загружать из Airflow Variables
# AUTORU_URLS = Variable.get("AUTORU_URLS", default_var='[]', deserialize_json=True)

# ============================================
# КЛАСС ДЛЯ ПАРСИНГА URL
# ============================================
@dataclass
class AutoRuUrlInfo:
    """Информация о URL Auto.ru"""
    full_url: str
    brand: str
    model: str
    body_id: str
    condition: str  # 'new' или 'used'
    task_id: str    # Уникальный ID для задачи Airflow
    
    @classmethod
    def from_url(cls, url: str, index: int) -> 'AutoRuUrlInfo':
        """Парсит URL и извлекает информацию"""
        parsed = urlparse(url)
        path_parts = parsed.path.strip('/').split('/')
        
        # Пример: /cars/bmw/m3/23978803/new/
        if len(path_parts) >= 5:
            brand = path_parts[1]  # bmw
            model = path_parts[2]  # m3
            body_id = path_parts[3]  # 23978803
            condition = path_parts[4]  # new
        else:
            # Если URL в другом формате, используем значения по умолчанию
            brand = 'unknown'
            model = 'unknown'
            body_id = str(index)
            condition = 'used'
        
        # Генерируем уникальный ID задачи
        task_id = f"parse_{brand}_{model}_{body_id}"
        
        return cls(
            full_url=url,
            brand=brand,
            model=model,
            body_id=body_id,
            condition=condition,
            task_id=task_id
        )

# ============================================
# КОНФИГУРАЦИЯ DAG
# ============================================
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=10),  # Таймаут для каждой задачи
}

dag = DAG(
    'auto_ru_dynamic_loader',
    default_args=default_args,
    description='Динамическая загрузка JSON данных из Auto.ru для разных моделей',
    schedule_interval='0 13 * * *',  # Ежедневно в 13:00
    catchup=False,
    tags=['clickhouse', 'auto.ru', 'dynamic', 'cars'],
    max_active_runs=1,
    # Важно: динамические DAG должны быть сериализуемыми
    render_template_as_native_obj=True,
)

# ============================================
# КЛАСС ДЛЯ РАБОТЫ С CLICKHOUSE
# ============================================
class ClickHouseJsonLoader:
    """Класс для загрузки JSON данных в ClickHouse"""
    
    def __init__(self, host: str = None, port: int = None, 
                 user: str = None, password: str = None,
                 database: str = None):
        
        # Получаем настройки из Airflow Variables или используем значения по умолчанию
        self.host = host or Variable.get("CLICKHOUSE_HOST", "ch1")
        self.port = port or int(Variable.get("CLICKHOUSE_PORT", "8123"))
        self.user = user or Variable.get("CLICKHOUSE_USER", "admin")
        self.password = password or Variable.get("CLICKHOUSE_PASSWORD", "password")
        self.database = database or Variable.get("CLICKHOUSE_DATABASE", "default")
        self.base_url = f"http://{self.host}:{self.port}"
        
        logger.info(f"ClickHouse loader initialized for {self.host}:{self.port}")
    
    def test_connection(self, **context) -> bool:
        """Проверка подключения к ClickHouse (общая для всех задач)"""
        try:
            response = requests.get(f"{self.base_url}/ping", timeout=10)
            
            if response.status_code == 200:
                logger.info(f"✅ Подключение к ClickHouse {self.host} успешно")
                return True
            else:
                logger.error(f"❌ Ошибка подключения к ClickHouse: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к ClickHouse {self.host}: {e}")
            raise AirflowException(f"Ошибка подключения к ClickHouse: {e}")
    
    def fetch_json_from_url(self, url_info: AutoRuUrlInfo, **context) -> Optional[Dict]:
        """Получает JSON-LD данные для конкретной ссылки"""
        task_instance = context.get('task_instance')
        url = url_info.full_url
        
        logger.info(f"🔍 Задача {url_info.task_id}: парсим {url}")
        
        try:
            # Получаем страницу
            response = requests.get(url, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"❌ Ошибка HTTP {response.status_code} для {url}")
                # Не выбрасываем исключение, чтобы другие задачи продолжали работу
                return None
            
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
                # Добавляем метаинформацию из URL
                product_data['_url_info'] = {
                    'brand': url_info.brand,
                    'model': url_info.model,
                    'body_id': url_info.body_id,
                    'condition': url_info.condition,
                    'source_url': url
                }
                
                name = product_data.get('name', 'Неизвестно')
                json_size = len(json.dumps(product_data))
                
                logger.info(f"📝 Название: {name}")
                logger.info(f"📏 Размер данных: {json_size:,} байт")
                
                # Сохраняем метаданные в XCom
                if task_instance:
                    task_instance.xcom_push(
                        key=f'{url_info.task_id}_name', 
                        value=name
                    )
                    task_instance.xcom_push(
                        key=f'{url_info.task_id}_size', 
                        value=json_size
                    )
                    task_instance.xcom_push(
                        key=f'{url_info.task_id}_url', 
                        value=url
                    )
                    # Возвращаемое значение также сохраняется в XCom
                
                return product_data
            else:
                logger.warning("❌ Не удалось получить JSON данные")
                return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения JSON для {url}: {e}")
            # Продолжаем выполнение других задач даже при ошибке
            return None
    
    def insert_json_data(self, url_info: AutoRuUrlInfo, **context) -> bool:
        """Вставляет JSON данные для конкретной ссылки в ClickHouse"""
        task_instance = context.get('task_instance')
        
        # Получаем данные из предыдущей задачи через XCom
        ti = context['ti']
        json_data = ti.xcom_pull(
            task_ids=url_info.task_id,  # ID задачи парсинга
            key='return_value'
        )
        
        if not json_data:
            logger.error(f"❌ Нет данных для вставки для {url_info.full_url}")
            return False
        
        try:
            # Преобразуем JSON в строку
            json_str = json.dumps(json_data, ensure_ascii=False)
            
            # Экранируем одинарные кавычки для SQL
            json_str_escaped = json_str.replace("'", "''")
            
            # Формируем INSERT запрос
            query = f"""
            INSERT INTO {self.database}.car_raw_json_data 
            (source, brand, model, body_id, condition, query_dttm, raw_json, source_url)
            VALUES (
                'auto.ru',
                '{url_info.brand}',
                '{url_info.model}',
                '{url_info.body_id}',
                '{url_info.condition}',
                now(),
                '{json_str_escaped}',
                '{url_info.full_url}'
            )
            """
            
            logger.info(f"📤 Записываем JSON данные в ClickHouse...")
            logger.info(f"📏 Размер JSON: {len(json_str):,} байт")
            
            # Выполняем запрос
            response = requests.post(
                self.base_url,
                params={'query': query},
                auth=(self.user, self.password),
                timeout=60
            )
            
            if response.status_code == 200:
                logger.info(f"✅ JSON успешно записан в ClickHouse для {url_info.brand} {url_info.model}")
                
                # Сохраняем метаданные в XCom
                if task_instance:
                    task_instance.xcom_push(
                        key=f'{url_info.task_id}_insert_status', 
                        value='success'
                    )
                
                return True
            else:
                logger.error(f"❌ Не удалось записать данные в ClickHouse: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка вставки данных для {url_info.full_url}: {e}")
            return False

# ============================================
# ДИНАМИЧЕСКОЕ СОЗДАНИЕ ЗАДАЧ
# ============================================

# 1. Парсим все URL и создаем объекты с информацией
url_infos = [AutoRuUrlInfo.from_url(url, i) for i, url in enumerate(AUTORU_URLS)]

logger.info(f"📋 Всего URL для обработки: {len(url_infos)}")
for info in url_infos:
    logger.info(f"   - {info.brand}/{info.model}/{info.body_id} ({info.condition})")

# 2. Создаем общие задачи
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Проверка подключения (одна на все задачи)
check_connection_task = PythonOperator(
    task_id='check_clickhouse_connection',
    python_callable=lambda **context: ClickHouseJsonLoader().test_connection(**context),
    dag=dag,
)

# 3. Создаем динамические задачи для каждой ссылки
parse_tasks = {}
insert_tasks = {}

for url_info in url_infos:
    # Создаем оператор для парсинга
    parse_task = PythonOperator(
        task_id=url_info.task_id,
        python_callable=lambda url_info=url_info, **context: 
            ClickHouseJsonLoader().fetch_json_from_url(url_info, **context),
        dag=dag,
        # Настраиваем таймаут и повторные попытки для каждой задачи
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
        # Продолжать выполнение других задач даже при ошибке
        trigger_rule='all_done',
    )
    
    # Создаем оператор для вставки
    insert_task = PythonOperator(
        task_id=f"insert_{url_info.task_id}",
        python_callable=lambda url_info=url_info, **context: 
            ClickHouseJsonLoader().insert_json_data(url_info, **context),
        dag=dag,
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=3),
        trigger_rule='all_done',  # Запускается даже если парсинг не удался
    )
    
    # Сохраняем задачи в словари
    parse_tasks[url_info.task_id] = parse_task
    insert_tasks[url_info.task_id] = insert_task

# 4. Задача для агрегации результатов
def aggregate_results(**context):
    """Собирает результаты всех задач и формирует отчет"""
    ti = context['ti']
    
    successful_parses = 0
    successful_inserts = 0
    total_urls = len(url_infos)
    
    for url_info in url_infos:
        # Проверяем результат парсинга
        parse_result = ti.xcom_pull(
            task_ids=url_info.task_id,
            key='return_value'
        )
        
        if parse_result:
            successful_parses += 1
        
        # Проверяем результат вставки
        insert_status = ti.xcom_pull(
            task_ids=f"insert_{url_info.task_id}",
            key=f'{url_info.task_id}_insert_status'
        )
        
        if insert_status == 'success':
            successful_inserts += 1
    
    # Формируем отчет
    report = f"""
    📊 ОТЧЕТ ВЫПОЛНЕНИЯ DAG auto_ru_dynamic_loader
    
    Общая статистика:
    - Всего URL: {total_urls}
    - Успешно распарсено: {successful_parses} ({successful_parses/total_urls*100:.1f}%)
    - Успешно записано в БД: {successful_inserts} ({successful_inserts/total_urls*100:.1f}%)
    
    Детали по моделям:
    """
    
    # Добавляем детали по каждой модели
    for url_info in url_infos:
        name = ti.xcom_pull(task_ids=url_info.task_id, key=f'{url_info.task_id}_name')
        size = ti.xcom_pull(task_ids=url_info.task_id, key=f'{url_info.task_id}_size')
        
        report += f"\n    • {url_info.brand} {url_info.model}: "
        if name:
            report += f"{name} ({size or 0:,} байт)"
        else:
            report += "❌ не удалось распарсить"
    
    logger.info(report)
    
    # Сохраняем отчет в XCom
    ti.xcom_push(key='execution_report', value=report)
    
    # Если ни одна задача не выполнилась успешно, можно выбросить исключение
    if successful_parses == 0:
        logger.warning("⚠️ Ни один URL не был успешно распарсен")
    elif successful_inserts == 0:
        logger.warning("⚠️ Ни один JSON не был записан в БД")

aggregate_task = PythonOperator(
    task_id='aggregate_results',
    python_callable=aggregate_results,
    dag=dag,
    trigger_rule='all_done',  # Запускается всегда
)

# 5. Финальные задачи
end_task = DummyOperator(
    task_id='end',
    dag=dag,
    trigger_rule='all_done',
)

# ============================================
# НАСТРОЙКА ЗАВИСИМОСТЕЙ
# ============================================

# Общая цепочка: старт -> проверка подключения
start_task >> check_connection_task

# Для каждой ссылки создаем свою цепочку
for url_info in url_infos:
    # check_connection -> parse -> insert
    check_connection_task >> parse_tasks[url_info.task_id] >> insert_tasks[url_info.task_id]
    
    # Все insert задачи ведут к агрегации
    insert_tasks[url_info.task_id] >> aggregate_task

# Агрегация завершает DAG
aggregate_task >> end_task

# ============================================
# ФУНКЦИИ УВЕДОМЛЕНИЙ
# ============================================
def send_success_notification(**context):
    """Отправляет уведомление об успешном выполнении"""
    try:
        ti = context['ti']
        report = ti.xcom_pull(task_ids='aggregate_results', key='execution_report')
        
        if report:
            logger.info(report)
        else:
            logger.info("✅ DAG auto_ru_dynamic_loader выполнен")
        
        # Здесь можно добавить отправку email/slack/etc
        # send_email(subject="DAG успешно выполнен", body=report)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления: {e}")

def send_failure_notification(context):
    """Отправляет уведомление об ошибке"""
    try:
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        exception = context.get('exception', 'Неизвестная ошибка')
        
        message = f"""
        ❌ DAG {dag_id} завершился с ошибкой в задаче {task_id}!
        
        Ошибка: {exception}
        
        Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        
        logger.error(message)
        
        # Здесь можно добавить отправку email/slack/etc
        # send_email(subject="Ошибка выполнения DAG", body=message)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления об ошибке: {e}")

# Назначаем обработчики
dag.on_success_callback = send_success_notification
dag.on_failure_callback = send_failure_notification

# ============================================
# АЛЬТЕРНАТИВНЫЙ ВАРИАНТ: ПАРАЛЛЕЛЬНОЕ ВЫПОЛНЕНИЕ
# ============================================
# Если нужно выполнять все задачи параллельно (без зависимостей между ними),
# можно использовать такой подход:

"""
# Создаем группу параллельных задач
from airflow.utils.helpers import chain

# Все задачи парсинга выполняются параллельно после проверки подключения
check_connection_task.set_downstream(list(parse_tasks.values()))

# Все задачи вставки выполняются параллельно после соответствующих задач парсинга
for url_info in url_infos:
    parse_tasks[url_info.task_id].set_downstream(insert_tasks[url_info.task_id])

# Все задачи вставки ведут к агрегации
for task in insert_tasks.values():
    task.set_downstream(aggregate_task)

aggregate_task.set_downstream(end_task)
"""

logger.info(f"✅ DAG создан с {len(url_infos)} динамическими задачами")