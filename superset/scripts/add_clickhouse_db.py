
#!/usr/bin/env python3
"""
Скрипт для добавления базы ClickHouse в Apache Superset
"""
import sys
import os

# Добавляем пути
sys.path.insert(0, '/app/superset_home/.local/lib/python3.10/site-packages')
sys.path.insert(0, '/app')

def add_clickhouse_database():
    """Добавляет базу ClickHouse в Superset"""
    try:
        from superset import app
        from superset.extensions import db
        from superset.models.core import Database
        
        with app.app_context():
            # Проверяем нет ли уже такой базы
            existing = db.session.query(Database).filter_by(database_name='ClickHouse Main').first()
            
            if existing:
                print(f"✅ База 'ClickHouse Main' уже существует (ID: {existing.id})")
                return True
            
            # Создаем новую базу
            database = Database(
                database_name='ClickHouse Main',
                sqlalchemy_uri='clickhouse://default:password@ch1:8123/default',
                extra='{"engine_params": {"connect_args": {"secure": false}}}'
            )
            
            db.session.add(database)
            db.session.commit()
            
            print(f"✅ База данных 'ClickHouse Main' успешно добавлена (ID: {database.id})")
            
            # Проверяем подключение
            try:
                from sqlalchemy import create_engine, text
                engine = create_engine('clickhouse://default:password@ch1:8123/default')
                with engine.connect() as conn:
                    result = conn.execute(text('SELECT version()'))
                    version = result.fetchone()[0]
                    print(f"✅ Подключение к ClickHouse успешно. Версия: {version}")
            except Exception as e:
                print(f"⚠️  База добавлена, но тест подключения не удался: {str(e)[:100]}")
            
            return True
            
    except Exception as e:
        print(f"❌ Ошибка при добавлении базы ClickHouse: {e}")
        return False

def test_clickhouse_connection():
    """Тестирует подключение к ClickHouse"""
    print("\n🔍 Тестирование подключения к ClickHouse...")
    
    try:
        import clickhouse_connect
        
        # Пробуем разные хосты
        hosts = ['ch1', 'clickhouse', 'localhost']
        
        for host in hosts:
            try:
                print(f"  Пробуем подключиться к {host}:8123...")
                client = clickhouse_connect.get_client(
                    host=host,
                    port=8123,
                    username='default',
                    password='password',
                    connect_timeout=5
                )
                
                version = client.query('SELECT version()').result_rows[0][0]
                print(f"  ✅ {host}: Успех! ClickHouse {version}")
                
                # Покажем базы
                dbs = client.query('SHOW DATABASES')
                print(f"    Доступные базы: {[db[0] for db in dbs.result_rows]}")
                
                return host
                
            except Exception as e:
                print(f"  ❌ {host}: {str(e)[:80]}")
                continue
                
        print("  ⚠️  Не удалось подключиться ни к одному из хостов")
        return None
        
    except ImportError:
        print("  ❌ Модуль clickhouse_connect не установлен")
        return None

if __name__ == "__main__":
    print("=== Добавление ClickHouse в Apache Superset ===")
    
    # Тестируем подключение
    working_host = test_clickhouse_connection()
    
    if working_host:
        print(f"\n🎯 Используем хост: {working_host}")
        
        # Обновляем URI если нашли рабочий хост
        if working_host != 'ch1':
            print(f"Обновляем URI на использование {working_host}")
            # Здесь можно обновить конфигурацию
        
        # Добавляем базу
        success = add_clickhouse_database()
        
        if success:
            print("\n✨ Готово! База ClickHouse добавлена в Superset.")
            print("   Откройте http://localhost:8088 и перейдите в Data → Databases")
        else:
            print("\n💥 Не удалось добавить базу ClickHouse")
            sys.exit(1)
    else:
        print("\n⚠️  Не удалось подключиться к ClickHouse")
        print("   Проверьте что:")
        print("   1. ClickHouse запущен")
        print("   2. Сеть Docker настроена правильно")
        print("   3. Пароль правильный (пробуйте пустой пароль если нужно)")
        sys.exit(1)
