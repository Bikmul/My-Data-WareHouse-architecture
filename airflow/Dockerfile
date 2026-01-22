# Dockerfile
FROM apache/airflow:2.7.1

USER root

# Установите системные утилиты
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        vim \
        netcat \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Установите Python пакеты для работы с ClickHouse
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    clickhouse-connect==0.10.0 \
    clickhouse-sqlalchemy==0.3.2 \
    requests==2.31.0 \
    pandas==2.1.4