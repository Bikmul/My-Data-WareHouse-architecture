cat > Makefile << 'EOF'
.PHONY: help build up down restart logs init test clean

help:
	@echo "Команды для управления Apache Superset:"
	@echo ""
	@echo "  build    - Сборка Docker образа"
	@echo "  up       - Запуск всех сервисов"
	@echo "  down     - Остановка всех сервисов"
	@echo "  restart  - Перезапуск Superset"
	@echo "  logs     - Просмотр логов"
	@echo "  init     - Полная инициализация"
	@echo "  test     - Тестирование подключения"
	@echo "  clean    - Полная очистка"
	@echo ""

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

restart:
	docker-compose restart superset

logs:
	docker-compose logs -f superset

init:
	./scripts/init_superset.sh

test:
	docker-compose exec superset python3 /app/scripts/test_connection.py

clean:
	docker-compose down -v
	docker image prune -f

install-drivers:
	docker-compose exec superset pip install --user \
		psycopg2-binary clickhouse-connect clickhouse-sqlalchemy

status:
	@echo "=== СТАТУС СЕРВИСОВ ==="
	docker-compose ps
	@echo ""
	@echo "=== ПРОВЕРКА SUPERSET ==="
	@curl -s http://localhost:8088/health > /dev/null && \
		echo "✅ Superset работает" || echo "❌ Superset не отвечает"
EOF