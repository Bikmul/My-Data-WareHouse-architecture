#!/bin/bash
# deploy-cluster.sh

echo "Создаем структуру папок..."
mkdir -p {clickhouse01,clickhouse02,clickhouse03,clickhouse04}/{config,data,logs}
mkdir -p zookeeper{1,2,3}/{data,datalog,logs}

echo "Копируем конфигурационные файлы..."
# Копируем users.xml для всех нод
cp clickhouse01/config/users.xml clickhouse02/config/users.xml
cp clickhouse01/config/users.xml clickhouse03/config/users.xml  
cp clickhouse01/config/users.xml clickhouse04/config/users.xml

echo "Запускаем кластер..."
docker-compose up -d

echo "Ждем запуска сервисов..."
sleep 30

echo "Проверяем статус контейнеров..."
docker-compose ps

echo "Проверяем подключение к ClickHouse нодам..."
for port in 8123 8124 8125 8126; do
    echo "Порт $port: $(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/ping)"
done

echo "Готово! Кластер запущен."
echo "Доступные ноды:"
echo " - ch1: http://localhost:8123"
echo " - ch2: http://localhost:8124" 
echo " - ch3: http://localhost:8125"
echo " - ch4: http://localhost:8126"