# Базовая конфигурация Apache Superset

import os
import sys
from celery.schedules import crontab
from superset.tasks.types import FixedExecutor

# Добавляем путь к пользовательским пакетам
USER_SITE = '/app/superset_home/.local/lib/python3.10/site-packages'
if USER_SITE not in sys.path:
    sys.path.insert(0, USER_SITE)

# ====== ОСНОВНЫЕ НАСТРОЙКИ ======
SQLALCHEMY_DATABASE_URI = 'postgresql://superset:superset@superset-db:5432/superset'
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'your-secret-key-change-this-in-production')

# ====== НАСТРОЙКИ БЕЗОПАСНОСТИ ======
TALISMAN_ENABLED = False
WEBDRIVER_BASEURL = "http://superset-app:8088"

# ====== ALERTS И REPORTS ======
ALERT_REPORTS_NOTIFICATION_DRY_RUN = False
ALERT_REPORTS_EXECUTORS = [FixedExecutor("admin")]

# ====== WEBDRIVER НАСТРОЙКИ ДЛЯ FIREFOX ======
WEBDRIVER_TYPE = "firefox"

WEBDRIVER_OPTION_ARGS = [
    "--headless",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--window-size=1920,1080"
]

WEBDRIVER_WINDOW = {
    "dashboard": (1920, 1080),
    "chart": (1920, 1080),
    "explore": (1920, 1080),
}

WEBDRIVER_TIMEOUT = 60
SCREENSHOT_LOAD_WAIT = 30
SCREENSHOT_LOCATE_WAIT = 10

# Включаем CORS
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*']
}

# Отключаем Content Security Policy
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

# CSRF настройки
WTF_CSRF_ENABLED = False
WTF_CSRF_TIME_LIMIT = 3600

# ====== ФУНКЦИОНАЛЬНОСТЬ ======
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_DND_WITH_CLICK_UX": True,
    "ENABLE_EXPLORE_DRAG_AND_DROP": True,
    "ENABLE_JAVASCRIPT_CONTROLS": True,
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
MAX_CONTENT_LENGTH = 16 * 1024 * 1024

# Для отладки
DEBUG = True
TEMPLATES_AUTO_RELOAD = True
PRESERVE_CONTEXT_ON_EXCEPTION = True
SEND_FILE_MAX_AGE_DEFAULT = 0

# ====== НАСТРОЙКИ ALERTS И REPORTS ======
ENABLE_ALERTS = True
ENABLE_SCHEDULED_EMAIL_REPORTS = True

# Настройки Celery
CELERY_CONFIG = {
    "broker_url": "redis://superset-redis:6379/0",
    "result_backend": "redis://superset-redis:6379/0",
    "imports": ["superset.tasks.scheduler"],
    "task_serializer": "json",
    "result_serializer": "json",
    "accept_content": ["json"],
    "task_ignore_result": True,
    "worker_prefetch_multiplier": 1,
    "task_acks_late": True,
    "timezone": "UTC",
    "beat_schedule": {
        "reports.scheduler": {
            "task": "reports.scheduler",
            "schedule": crontab(minute="*", hour="*"),
        },
        "reports.prune_log": {
            "task": "reports.prune_log",
            "schedule": crontab(minute=0, hour=0),
        },
    },
}

# ====== НАСТРОЙКИ SMTP ======
EMAIL_NOTIFICATIONS = True
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_STARTTLS = True
SMTP_SSL = False
SMTP_USER = os.environ.get('SMTP_USER', '')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '')
SMTP_MAIL_FROM = SMTP_USER

# Настройки для тестирования
ALERT_REPORTS_NOTIFICATION_LIMIT = 100
ALERT_REPORTS_CRON_LIMIT = 100

# Время ожидания для задач
SCHEDULED_EMAIL_REPORTS_TASK_TIMEOUT = 300
ALERT_REPORTS_TASK_TIMEOUT = 300

print("[Superset Config] Конфигурация загружена")
print("[Superset Config] WEBDRIVER_TYPE: firefox")