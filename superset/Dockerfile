FROM apache/superset:latest

USER root

# Установка Firefox и GeckoDriver
RUN apt-get update && \
    apt-get install -y \
    firefox-esr \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Установка GeckoDriver
RUN wget -q https://github.com/mozilla/geckodriver/releases/download/v0.34.0/geckodriver-v0.34.0-linux64.tar.gz && \
    tar -xzf geckodriver-v0.34.0-linux64.tar.gz -C /usr/local/bin && \
    rm geckodriver-v0.34.0-linux64.tar.gz && \
    chmod +x /usr/local/bin/geckodriver

# Переключаемся на пользователя superset
USER superset

# Установка Python пакетов
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    clickhouse-connect==0.10.0 \
    clickhouse-sqlalchemy==0.3.2 \
    selenium==4.15.0

# Копируем конфигурационные файлы
USER root
COPY superset_config.py /app/pythonpath/
COPY clickhouse_init.py /app/pythonpath/
RUN chown -R superset:superset /app/pythonpath/
USER superset

# Проверка Firefox
RUN firefox --version && geckodriver --version