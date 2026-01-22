import requests
import json
import re
import datetime
from typing import Dict, Any, Optional

class ClickHouseJsonWriter:
    """Класс для записи JSON данных в ClickHouse"""
    
    def __init__(self, host: str = "localhost", port: int = 8123, 
                 user: str = "admin", password: str = "password",
                 database: str = "default"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.base_url = f"http://{host}:{port}"
    
    def test_connection(self) -> bool:
        """Проверка подключения к ClickHouse"""
        try:
            response = requests.get(f"{self.base_url}/ping", timeout=5)
            return response.status_code == 200
        except Exception as e:
            print(f"❌ Ошибка подключения к ClickHouse: {e}")
            return False
    
    def execute_query(self, query: str) -> Optional[Dict]:
        """Выполняет запрос к ClickHouse"""
        try:
            response = requests.post(
                self.base_url,
                params={'query': query},
                auth=(self.user, self.password),
                timeout=30
            )
            
            if response.status_code == 200:
                return {'success': True, 'data': response.text}
            else:
                print(f"❌ Ошибка запроса ({response.status_code}): {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка выполнения запроса: {e}")
            return None
    
    def insert_json_data(self, json_data: Dict, source: str = "auto.ru") -> bool:
        """Вставляет JSON данные одной записью в ClickHouse"""
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
                '{source}',
                now(),
                '{json_str_escaped}'
            )
            """
            
            print(f"📤 Записываем JSON данные в ClickHouse...")
            print(f"📏 Размер JSON: {len(json_str):,} байт")
            
            result = self.execute_query(query)
            if result and result.get('success'):
                print("✅ JSON успешно записан в ClickHouse")
                return True
            else:
                print("❌ Не удалось записать данные")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка вставки данных: {e}")
            return False
    
    def get_table_info(self) -> None:
        """Получает информацию о таблице"""
        query = f"""
        SELECT 
            count() as total_rows,
            min(query_dttm) as first_record,
            max(query_dttm) as last_record,
            avg(length(raw_json)) as avg_json_size,
            max(length(raw_json)) as max_json_size
        FROM {self.database}.car_raw_json_data
        """
        
        result = self.execute_query(query)
        if result:
            print(f"\n📊 Информация о таблице car_raw_json_data:")
            print(result['data'])

def get_json_ld_from_url(url: str) -> Optional[Dict]:
    """Получает JSON-LD данные с указанного URL"""
    print(f"\n🔍 Получаем JSON-LD с: {url}")
    
    try:
        # Получаем страницу
        response = requests.get(url, timeout=10)
        
        if response.status_code != 200:
            print(f"❌ Ошибка {response.status_code}")
            return None
        
        print(f"✅ Страница загружена ({len(response.text):,} байт)")
        
        # Ищем JSON-LD блоки
        pattern = r'<script\s+type="application/ld\+json">\s*({.*?})\s*</script>'
        matches = re.findall(pattern, response.text, re.DOTALL)
        
        if not matches:
            print("⚠️ JSON-LD данные не найдены")
            return None
        
        print(f"✅ Найдено JSON-LD блоков: {len(matches)}")
        
        # Ищем блок с типом Product (или берем первый)
        product_data = None
        for i, json_str in enumerate(matches, 1):
            try:
                data = json.loads(json_str)
                # print(f"📄 Блок #{i}: тип = {data.get('@type', 'Неизвестно')}")
                
                if data.get('@type') == 'Product':
                    product_data = data
                    print(f"🎯 Используем блок с @type='Product'")
                    break
            except json.JSONDecodeError as e:
                print(f"⚠️ Ошибка декодирования JSON: {e}")
                continue
        
        # Если не нашли Product, берем первый валидный блок
        if not product_data and matches:
            try:
                product_data = json.loads(matches[0])
                print(f"📄 Используем первый JSON-LD блок")
            except:
                print("❌ Не удалось декодировать ни один JSON блок")
                return None
        
        if product_data:
            # print(f"📊 Тип данных: {product_data.get('@type', 'Неизвестно')}")
            print(f"📝 Название: {product_data.get('name', 'Неизвестно')}")
            # print(f"📏 Размер данных: {len(json.dumps(product_data)):,} байт")
            return product_data
        else:
            print("❌ Не удалось получить JSON данные")
            return None
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None

def main():
    print("="*60)
    print("ПОЛУЧЕНИЕ И СОХРАНЕНИЕ JSON В CLICKHOUSE")
    print("="*60)
    
    # Инициализируем ClickHouse writer
    ch_writer = ClickHouseJsonWriter()
    
    # Проверяем подключение
    print("🔌 Проверяем подключение к ClickHouse...")
    if not ch_writer.test_connection():
        print("❌ Не удалось подключиться к ClickHouse")
        return
    
    print("✅ Подключение к ClickHouse установлено")
    
    # URL для парсинга
    url = "https://auto.ru/cars/bmw/m3/23978803/new/?output_type=list"
    
    # 1. Получаем JSON данные
    json_data = get_json_ld_from_url(url)
    
    if json_data:
        # 2. Сохраняем JSON в ClickHouse
        success = ch_writer.insert_json_data(json_data, source="auto.ru")
        
        if success:
            # 3. Показываем информацию о таблице
            ch_writer.get_table_info()
            
            # 4. Сохраняем резервную копию в файл
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"json_backup_{timestamp}.json"
            
    else:
        print("\n❌ Не удалось получить JSON данные")
    
    print("\n" + "="*60)
    print("✅ ВЫПОЛНЕНИЕ ЗАВЕРШЕНО")
    print("="*60)


if __name__ == "__main__":
    # Основной запуск
    main()
    
