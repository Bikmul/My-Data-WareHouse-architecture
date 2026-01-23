
FROM apache/superset:latest

# Устанавливаем драйверы в виртуальное окружение Superset
USER superset

# Проверяем что мы в правильном окружении
RUN python3 -c "import sys; print('Python executable:', sys.executable)"

# Устанавливаем драйверы (будут в /app/superset_home/.local/lib/python3.10/site-packages)
RUN pip install --user \
    psycopg2-binary==2.9.9 \
    clickhouse-connect==0.10.0 \
    clickhouse-sqlalchemy==0.3.2

# Проверяем установку
RUN cd /app/superset_home/.local/lib/python3.10/site-packages && \
    python3 -c "import sys; sys.path.insert(0, '.'); import psycopg2; print('psycopg2 OK')" && \
    python3 -c "import sys; sys.path.insert(0, '.'); import clickhouse_connect; print('clickhouse_connect OK')"

# Копируем конфигурационные файлы
USER root
COPY superset_config.py /app/pythonpath/
COPY clickhouse_init.py /app/pythonpath/
RUN chown superset:superset /app/pythonpath/*.py
USER superset