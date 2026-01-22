FROM apache/superset:latest

# Установите в виртуальное окружение
RUN /app/.venv/bin/pip install clickhouse-connect clickhouse-sqlalchemy

# Проверьте
RUN /app/.venv/bin/python -c "import clickhouse_connect; print('clickhouse_connect установлен')"
EOF