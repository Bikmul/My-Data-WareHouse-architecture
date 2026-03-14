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

# ====== ОСНОВНЫЕ НАСТРОЙКИ ======

# Настройки базы данных
SQLALCHEMY_DATABASE_URI = 'postgresql://superset:superset@superset-db:5432/superset'

# Секретный ключ
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'your-secret-key-change-this-in-production')

# ====== НАСТРОЙКИ БЕЗОПАСНОСТИ (ДЛЯ ИЗОБРАЖЕНИЙ И HANDLEBARS) ======

# ОТКЛЮЧАЕМ TALISMAN - главный блокировщик внешних ресурсов
TALISMAN_ENABLED = False

# Включаем CORS для внешних ресурсов
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*']
}

# ОТКЛЮЧАЕМ Content Security Policy (CSP)
CONTENT_SECURITY_POLICY = None
CONTENT_SECURITY_POLICY_WARNING = False
CONTENT_SECURITY_POLICY_REPORT_ONLY = False

# HTTP заголовки
HTTP_HEADERS = {
    'X-Frame-Options': 'ALLOWALL',
    'X-Content-Type-Options': 'nosniff',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
}

# Настройки прокси
ENABLE_PROXY_FIX = True
PROXY_FIX_CONFIG = {
    'x_for': 1,
    'x_proto': 1,
    'x_host': 1,
    'x_port': 1,
    'x_prefix': 1
}

# CSRF настройки (можно включить позже)
WTF_CSRF_ENABLED = False  # Временно отключаем для теста
WTF_CSRF_TIME_LIMIT = 3600

# ====== ФУНКЦИОНАЛЬНОСТЬ ======

FEATURE_FLAGS = {
    # Включаем Handlebars шаблонизатор
    "ENABLE_TEMPLATE_PROCESSING": True,
    
    # Дополнительные функции
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_DND_WITH_CLICK_UX": True,
    "ENABLE_EXPLORE_DRAG_AND_DROP": True,
    "ENABLE_JAVASCRIPT_CONTROLS": True,
    
    # Для лучшей работы с данными
    "ALERT_REPORTS": True,
    "OMNIBAR": True,
    "DASHBOARD_RBAC": True,
}

# Настройки кэширования
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300
}

# Настройки загрузки файлов
UPLOAD_FOLDER = '/app/uploads'
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'svg'}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB

# Для отладки
DEBUG = True
TEMPLATES_AUTO_RELOAD = True
PRESERVE_CONTEXT_ON_EXCEPTION = True
SEND_FILE_MAX_AGE_DEFAULT = 0

print("[Superset Config] Конфигурация загружена")
print("[Superset Config] TALISMAN_ENABLED: False - внешние изображения разрешены")
print("[Superset Config] CSP: Отключен - JavaScript разрешен")
print("[Superset Config] Handlebars: Включен")