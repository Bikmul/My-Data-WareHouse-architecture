#!/usr/bin/env python3
"""
Тест подключения к ClickHouse
"""

import sys
import os

def test_python_environment():
    """Тестирует Python окружение"""
    print("=== ТЕСТ PYTHON ОКРУЖЕНИЯ ===")
    print(f"1. Python: {sys.executable}")
    print(f"2. Version: {sys.version}")
    
    # Проверяем пути
    print("\n3. Python пути (первые 5):")
    for i, path in enumerate(sys.path[:5]):
        print(f"   {i}. {path}")
    
    # Проверяем site-packages
    print("\n4. Проверка site-packages:")
    possible_paths = [
        '/app/superset_home/.local/lib/python3.10/site-packages',
        '/usr/local/lib/python3.10/site-packages',
        '/app/.venv/lib/python3.10/site-packages',
    ]
    
    for path in possible_paths:
        exists = os.path.exists(path)
        print(f"   {path}: {'✅ существует' if exists else '❌ нет'}")
        if exists:
            ch_path = os.path.join(path, 'clickhouse_connect')
            if os.path.exists(ch_path):
                print(f"        📍 clickhouse_connect найден!")
                if path not in sys.path:
                    sys.path.insert(0, path)
                    print(f"        ➕ Добавлен в sys.path")

def test_drivers():
    """Тестирует драйверы"""
    print("\n=== ТЕСТ ДРАЙВЕРОВ ===")
    
    # Psycopg2
    try:
        import psycopg2
        print(f"✅ psycopg2: OK")
    except ImportError as e:
        print(f"❌ psycopg2: {e}")
    
    # ClickHouse
    try:
        import clickhouse_connect
        print(f"✅ clickhouse_connect: {clickhouse_connect.__version__}")
        print(f"   Путь: {clickhouse_connect.__file__}")
        return True
    except ImportError as e:
        print(f"❌ clickhouse_connect: {e}")
        return False

def test_clickhouse_connection():
    """Тестирует подключение к ClickHouse"""
    print("\n=== ТЕСТ ПОДКЛЮЧЕНИЯ К CLICKHOUSE ===")
    
    try:
        import clickhouse_connect
        
        hosts = ['ch1', 'clickhouse', 'localhost', '172.17.0.1']
        
        for host in hosts:
            print(f"\nПробуем {host}:8123...")
            try:
                client = clickhouse_connect.get_client(
                    host=host,
                    port=8123,
                    username='default',
                    password='password',
                    connect_timeout=5
                )
                
                # Тест версии
                version = client.query('SELECT version()').result_rows[0][0]
                print(f"   ✅ УСПЕХ! ClickHouse {version}")
                
                # Покажем базы
                dbs = client.query('SHOW DATABASES')
                print(f"   📊 Базы данных:")
                for db in dbs.result_rows:
                    print(f"     - {db[0]}")
                
                return True
                
            except Exception as e:
                print(f"   ❌ Ошибка: {str(e)[:80]}")
                continue
                
        print("\n⚠️  Не удалось подключиться ни к одному хосту")
        return False
        
    except Exception as e:
        print(f"❌ Общая ошибка: {e}")
        return False

def main():
    """Основная функция"""
    print("🚀 ТЕСТ ПОДКЛЮЧЕНИЯ SUPERSET + CLICKHOUSE")
    print("=" * 50)
    
    # Тест окружения
    test_python_environment()
    
    # Тест драйверов
    drivers_ok = test_drivers()
    
    if drivers_ok:
        # Тест подключения
        connection_ok = test_clickhouse_connection()
        
        if connection_ok:
            print("\n" + "=" * 50)
            print("✨ ВСЁ РАБОТАЕТ КОРРЕКТНО!")
            print("=" * 50)
            return 0
        else:
            print("\n" + "=" * 50)
            print("⚠️  Драйверы установлены, но подключение не удалось")
            print("   Проверьте:")
            print("   1. Сеть Docker")
            print("   2. Доступность ClickHouse")
            print("   3. Пароль")
            print("=" * 50)
            return 1
    else:
        print("\n" + "=" * 50)
        print("❌ ДРАЙВЕРЫ НЕ УСТАНОВЛЕНЫ")
        print("   Выполните:")
        print("   docker-compose exec superset pip install --user \\")
        print("       psycopg2-binary clickhouse-connect clickhouse-sqlalchemy")
        print("=" * 50)
        return 2

if __name__ == "__main__":
    sys.exit(main())
