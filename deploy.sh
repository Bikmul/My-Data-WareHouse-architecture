#!/bin/bash

# Простой скрипт для запуска Data Warehouse архитектуры
# Работает с docker-compose в каждой папке

set -e  # Выход при ошибке

echo "=== Data Warehouse Deploy ==="
echo ""

# Функция для обработки одной службы
deploy_service() {
    local service_name=$1
    local compose_file=$2
    
    echo "🔧 Обработка: $service_name"
    
    if [ -f "$compose_file" ]; then
        echo "  ⏹ Останавливаем старые контейнеры..."
        docker-compose -f "$compose_file" down
        
        echo "  🚀 Запускаем новые контейнеры..."
        docker-compose -f "$compose_file" up -d
        
        echo "  ✅ $service_name готов!"
    else
        echo "  ❌ Файл $compose_file не найден!"
    fi
    
    echo ""
}

# Основной процесс
main() {
    echo "📁 Рабочая директория: $(pwd)"
    echo ""
    
    # 1. ClickHouse Cluster
    if [ -d "clickhouse-cluster-2x2" ]; then
        cd "clickhouse-cluster-2x2"
        deploy_service "ClickHouse Cluster" "docker-compose.yml"
        cd ..
    else
        echo "⚠ Папка clickhouse-cluster-2x2 не найдена"
    fi
    
    # 2. Apache Airflow
    if [ -d "airflow" ]; then
        cd "airflow"
        deploy_service "Apache Airflow" "docker-compose.yml"
        cd ..
    else
        echo "⚠ Папка airflow не найдена"
    fi
    
    # 3. Apache Superset
    if [ -d "superset" ]; then
        cd "superset"
        deploy_service "Apache Superset" "docker-compose.yml"
        cd ..
    else
        echo "⚠ Папка superset не найдена"
    fi
    
    echo "================================="
    echo "🎉 Все сервисы развернуты!"
    echo ""
    
    # Показываем статус
    echo "📊 Статус контейнеров:"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "(clickhouse|airflow|superset)"
}

# Запускаем основной процесс
main