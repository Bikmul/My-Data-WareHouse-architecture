# test_connection.py
import requests

print("="*50)
print("ПРОВЕРКА ПОДКЛЮЧЕНИЯ К AUTO.RU")
print("="*50)

# 1. Простейший запрос к главной странице
try:
    print("\n1. Проверяем главную страницу...")
    response = requests.get("https://auto.ru", timeout=10)
    print(f"   Статус: {response.status_code}")
    print(f"   Размер: {len(response.text)} байт")
    
    if response.status_code == 200:
        print("   ✅ Успешно!")
    else:
        print("   ⚠️  Что-то не так")
        
except Exception as e:
    print(f"   ❌ Ошибка: {e}")

# 2. Пробуем получить страницу с поиском BMW M3
try:
    print("\n2. Пробуем получить страницу поиска BMW M3...")
    url = "https://auto.ru/kazan/cars/bmw/m3/all/?transmission=AUTOMATIC"
    response = requests.get(url, timeout=10)
    print(f"   Статус: {response.status_code}")
    print(f"   Размер: {len(response.text)} байт")
    
    if response.status_code == 200:
        print("   ✅ Успешно!")
        
        # Простая проверка содержимого
        text = response.text.lower()
        if "bmw" in text and "m3" in text:
            print("   🚗 Найдены ключевые слова BMW M3")
        if "цена" in text or "price" in text:
            print("   💰 Есть информация о ценах")
            
    else:
        print("   ⚠️  Что-то не так")
        
except Exception as e:
    print(f"   ❌ Ошибка: {e}")

# 3. Сохраняем маленький кусочек HTML для анализа
try:
    print("\n3. Сохраняем образец HTML...")
    url = "https://auto.ru/cars/bmw/m3/23978803/new/?output_type=list"

    response = requests.get(url, timeout=10)
    
    if response.status_code == 200:
        # Берем только первые 5000 символов
        sample = response.text[:50000]
        with open("sample.html", "w", encoding="utf-8") as f:
            f.write(sample)
        print("   💾 Сохранено в sample.html")
        print("   📄 Первые 100 символов:")
        print("   " + "-"*40)
        print(f"   {sample[:100]}...")
    else:
        print("   ⚠️  Не удалось получить страницу")
        
except Exception as e:
    print(f"   ❌ Ошибка: {e}")

print("\n" + "="*50)
print("ПРОВЕРКА ЗАВЕРШЕНА")
print("="*50)