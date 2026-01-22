# parse_json_ld.py
import requests
import json
import re

def parse_json_ld_from_url(url):
    """Парсит JSON-LD данные с указанного URL"""
    print(f"\n🔍 Парсим JSON-LD с: {url}")
    
    try:
        # Получаем страницу
        response = requests.get(url, timeout=10)
        
        if response.status_code != 200:
            print(f"❌ Ошибка {response.status_code}")
            return None
        
        print(f"✅ Страница загружена ({len(response.text):,} байт)")
        
        # Ищем ВСЕ JSON-LD данные
        pattern = r'<script\s+type="application/ld\+json">\s*({.*?})\s*</script>'
        matches = re.findall(pattern, response.text, re.DOTALL)
        
        if not matches:
            print("⚠️ JSON-LD данные не найдены")
            return None
        
        print(f"✅ Найдено JSON-LD блоков: {len(matches)}")
        
        # Ищем блок с типом Product
        product_data = None
        for json_str in matches:
            try:
                data = json.loads(json_str)
                if data.get('@type') == 'Product':
                    product_data = data
                    print(f"🎯 Найден блок с @type='Product'")
                    break
            except json.JSONDecodeError as e:
                print(f"⚠️ Ошибка декодирования JSON: {e}")
                continue
        
        if not product_data:
            print("⚠️ Блок с @type='Product' не найден")
            return None
        
        print(f"📊 Тип данных: {product_data.get('@type', 'Неизвестно')}")
        print(f"📝 Название: {product_data.get('name', 'Неизвестно')}")
        
        # Сохраняем сырые данные
        with open('json_ld_raw.json', 'w', encoding='utf-8') as f:
            json.dump(product_data, f, ensure_ascii=False, indent=2)
        print("💾 Сырые данные сохранены в json_ld_raw.json")
        
        return product_data
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None

def extract_cars_from_json_ld(json_data):
    """Извлекает данные об автомобилях из JSON-LD"""
    
    if not json_data:
        return []
    
    print("\n🚗 Извлекаем данные об автомобилях...")
    
    cars = []
    
    try:
        # Проверяем структуру (теперь ищем в блоке Product)
        if 'offers' in json_data and 'offers' in json_data['offers']:
            offers = json_data['offers']['offers']
            print(f"✅ Найдено предложений: {len(offers)}")
            
            for i, offer in enumerate(offers, 1):
                try:
                    car = {
                        'number': i,
                        'name': offer.get('image', {}).get('name', 'Неизвестно'),
                        'price': offer.get('price', 0),
                        'currency': offer.get('priceCurrency', 'RUR'),
                        'url': offer.get('url', ''),
                        'seller': offer.get('image', {}).get('creator', {}).get('name', 'Неизвестно'),
                        'availability': offer.get('availability', ''),
                        'price_valid_until': offer.get('priceValidUntil', '')
                    }
                    cars.append(car)
                    
                except Exception as e:
                    print(f"⚠️ Ошибка парсинга предложения {i}: {e}")
                    continue
        
        # Также выводим общую статистику
        if 'offers' in json_data:
            aggregate = json_data['offers']
            print(f"\n📈 Общая статистика:")
            print(f"   Всего предложений: {aggregate.get('offerCount', 0)}")
            print(f"   Минимальная цена: {aggregate.get('lowPrice', 0):,} {aggregate.get('priceCurrency', 'RUR')}")
            print(f"   Максимальная цена: {aggregate.get('highPrice', 0):,} {aggregate.get('priceCurrency', 'RUR')}")
        
        # Рейтинг если есть
        if 'aggregateRating' in json_data:
            rating = json_data['aggregateRating']
            print(f"\n⭐ Рейтинг: {rating.get('ratingValue', 0)}/5")
            print(f"   Отзывов: {rating.get('reviewCount', 0)}")
        
    except Exception as e:
        print(f"❌ Ошибка извлечения данных: {e}")
    
    return cars

def display_cars(cars):
    """Отображает автомобили в консоли"""
    if not cars:
        print("\n📭 Автомобили не найдены")
        return
    
    print(f"\n{'='*60}")
    print(f"НАЙДЕНО АВТОМОБИЛЕЙ: {len(cars)}")
    print('='*60)
    
    for car in cars[:10]:  # Показываем первые 10
        print(f"\n🚗 #{car['number']}: {car['name']}")
        print(f"   💰 Цена: {car['price']:,} {car['currency']}")
        print(f"   👤 Продавец: {car['seller']}")
        print(f"   🔗 Ссылка: {car['url'][:60]}...")
        
        if car['price_valid_until']:
            print(f"   📅 Цена действительна до: {car['price_valid_until']}")
    
    # Если автомобилей больше 10, покажем статистику
    if len(cars) > 10:
        print(f"\n... и еще {len(cars) - 10} автомобилей")

def save_to_json(cars, filename='cars.json'):
    """Сохраняет автомобили в JSON файл"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(cars, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Данные сохранены в {filename}")
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")


# ==================== ОСНОВНАЯ ЧАСТЬ ====================

def main():
    print("="*60)
    print("ПАРСИНГ JSON-LD ДАННЫХ С AUTO.RU")
    print("="*60)
    
    # URL для парсинга (можно изменить)
    url = "https://auto.ru/cars/bmw/m3/23978803/new/?output_type=list"
    
    # 1. Получаем JSON-LD данные
    json_data = parse_json_ld_from_url(url)
    
    if json_data:
        # 2. Извлекаем автомобили
        cars = extract_cars_from_json_ld(json_data)
        
        # 3. Показываем результат
        display_cars(cars)
        
        # 4. Сохраняем в файлы
        if cars:
            save_to_json(cars)
            
            # Дополнительная информация
            print(f"\n📊 СТАТИСТИКА:")
            print(f"   Всего автомобилей: {len(cars)}")
            
            # Средняя цена
            if cars:
                avg_price = sum(c['price'] for c in cars) / len(cars)
                print(f"   Средняя цена: {avg_price:,.0f} {cars[0]['currency']}")
                
                # Минимальная и максимальная цены
                min_price = min(c['price'] for c in cars)
                max_price = max(c['price'] for c in cars)
                print(f"   Диапазон цен: {min_price:,} - {max_price:,} {cars[0]['currency']}")
    else:
        print("\n❌ Не удалось получить данные")
    
    print("\n" + "="*60)
    print("✅ ПАРСИНГ ЗАВЕРШЕН")
    print("="*60)

def load_to_clickhouse():
    """Минимальный список таблиц"""
    
    # Выполняем запрос
    response = requests.post(
        "http://ch1:8123",
        params={
            'query': """
            SELECT 
                database,
                name as table_name,
                engine
            FROM system.tables 
            WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema' )
            ORDER BY database, table_name
            """,
            'default_format': 'JSON'
        },
        auth=('admin', 'password'),
        timeout=10
    )
    
    if response.status_code == 200:
        data = response.json()
        print("Даннанные загружены ✅")

    else:
        print(f"Ошибка загрузки данных❌: {response.status_code}")

if __name__ == "__main__":
    # Основной парсинг
    main()
    