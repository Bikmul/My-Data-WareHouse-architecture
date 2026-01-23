#!/bin/bash
set -e

echo "=== ИНИЦИАЛИЗАЦИЯ APACHE SUPERSET ==="
echo "Контрольная точка: $(date)"

# 1. ОЧИСТКА
echo -e "\n1. Очистка..."
docker-compose down 2>/dev/null || true
docker volume rm superset_superset-home 2>/dev/null || true

# 2. СБОРКА ОБРАЗА
echo -e "\n2. Сборка Docker образа..."
docker-compose build --no-cache

# 3. ЗАПУСК БАЗЫ ДАННЫХ И REDIS
echo -e "\n3. Запуск PostgreSQL и Redis..."
docker-compose up -d superset-db superset-redis
sleep 30

# 4. ПРОВЕРКА БАЗЫ ДАННЫХ
echo -e "\n4. Проверка базы данных..."
docker-compose exec superset-db pg_isready -U superset

# 5. ИНИЦИАЛИЗАЦИЯ SUPERSET
echo -e "\n5. Инициализация Superset..."

echo "   a) Установка драйверов..."
docker-compose run --rm superset bash -c "
pip install --user psycopg2-binary clickhouse-connect clickhouse-sqlalchemy
echo 'Драйверы установлены'
"

echo "   b) Инициализация базы данных..."
docker-compose run --rm superset superset db upgrade

echo "   c) Создание администратора..."
docker-compose run --rm superset superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin

echo "   d) Инициализация..."
docker-compose run --rm superset superset init

# 6. ЗАПУСК SUPERSET
echo -e "\n6. Запуск Superset..."
docker-compose up -d superset
sleep 20

# 7. ПРОВЕРКА
echo -e "\n7. Проверка..."
if curl -s http://localhost:8088/health > /dev/null; then
    echo "✅ SUPERSET ЗАПУЩЕН!"
else
    echo "❌ Ошибка запуска Superset"
    echo "Логи:"
    docker-compose logs superset --tail=30
    exit 1
fi

# 8. ПОДКЛЮЧЕНИЕ К СЕТИ CLICKHOUSE
echo -e "\n8. Подключение к сети ClickHouse..."
if docker network ls | grep -q clickhouse-cluster-2x2_clickhouse_net; then
    docker network connect clickhouse-cluster-2x2_clickhouse_net superset-app
    echo "✅ Подключено к сети ClickHouse"
else
    echo "⚠️  Сеть ClickHouse не найдена"
fi

# 9. ТЕСТ ПОДКЛЮЧЕНИЯ
echo -e "\n9. Тест подключения к ClickHouse..."
docker-compose exec superset python3 /app/scripts/test_connection.py

# 10. ИТОГ
echo -e "\n"$(printf '=%.0s' {1..50})
echo "✨ ИНИЦИАЛИЗАЦИЯ ЗАВЕРШЕНА!"
echo $(printf '=%.0s' {1..50})
echo ""
echo "🌐 Адрес: http://localhost:8088"
echo "👤 Логин: admin"
echo "🔑 Пароль: admin"
echo ""
echo "⚡ Команды:"
echo "   • docker-compose logs -f superset    # Логи"
echo "   • docker-compose restart superset    # Перезапуск"
echo "   • ./scripts/test_connection.py       # Тест ClickHouse"
echo ""
echo "📊 Для добавления ClickHouse:"
echo "   1. Откройте http://localhost:8088"
echo "   2. Data → Databases → + DATABASE"
echo "   3. SQLAlchemy URI: clickhouse://default:password@ch1:8123/default"
echo $(printf '=%.0s' {1..50})
