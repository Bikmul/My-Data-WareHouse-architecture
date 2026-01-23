"""
Инициализация ClickHouse для Superset
Этот файл выполняется при запуске Superset
"""

import sys
import os

print("[ClickHouse Init] Загрузка инициализатора...")

# Путь к пользовательским пакетам
USER_SITE = '/app/superset_home/.local/lib/python3.10/site-packages'
if USER_SITE not in sys.path:
    sys.path.insert(0, USER_SITE)
    print(f"[ClickHouse Init] Добавлен путь: {USER_SITE}")

# Проверяем драйверы
try:
    import clickhouse_connect
    print(f"[ClickHouse Init] clickhouse_connect загружен")
except ImportError as e:
    print(f"[ClickHouse Init] ОШИБКА: clickhouse_connect не загружен: {e}")

try:
    import clickhouse_sqlalchemy
    print(f"[ClickHouse Init] clickhouse_sqlalchemy загружен")
except ImportError as e:
    print(f"[ClickHouse Init] ОШИБКА: clickhouse_sqlalchemy не загружен: {e}")

print("[ClickHouse Init] Инициализация завершена")
