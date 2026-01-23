# Базовая конфигурация Apache Superset

import os
import sys

# Добавляем путь к пользовательским пакетам
USER_SITE = '/app/superset_home/.local/lib/python3.10/site-packages'
if USER_SITE not in sys.path:
    sys.path.insert(0, USER_SITE)
    print(f"[Superset Config] Добавлен путь: {USER_SITE}")

# Проверяем доступность драйверов
try:
    import psycopg2
    print(f"[Superset Config] psycopg2 загружен")
except ImportError as e:
    print(f"[Superset Config] ВНИМАНИЕ: psycopg2 не загружен: {e}")

try:
    import clickhouse_connect
    print(f"[Superset Config] clickhouse_connect загружен")
except ImportError as e:
    print(f"[Superset Config] ВНИМАНИЕ: clickhouse_connect не загружен: {e}")

# Настройки базы данных
SQLALCHEMY_DATABASE_URI = 'postgresql://superset:superset@superset-db:5432/superset'

# Секретный ключ
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'your-secret-key-change-this-in-production')

# Безопасность
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = 3600

# Функциональность
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

print("[Superset Config] Конфигурация загружена")
