import asyncio
import aiohttp
import random
from datetime import date, timedelta
import string

# Настройки
ORDER_URL = "http://localhost/api/order/create/"  # Замените на ваш URL

# Вспомогательные функции для генерации данных
def random_date():
    """Случайная дата в ближайшие 30 дней"""
    days_ahead = random.randint(0, 30)
    return date.today() + timedelta(days=days_ahead)

def random_time():
    """Случайное время с 9:00 до 19:00 с шагом 15 минут"""
    hour = random.randint(9, 18)
    minute = random.choice([0, 15, 30, 45])
    return f"{hour:02d}:{minute:02d}"

def random_phone():
    """Генерирует случайный российский номер телефона"""
    return "+79" + "".join(random.choices(string.digits, k=9))

def random_name():
    """Простой генератор случайного имени (можно расширить)"""
    first_names = ["Алексей", "Мария", "Иван", "Анна", "Дмитрий", "Екатерина", "Сергей", "Ольга"]
    last_names = ["Иванов", "Петрова", "Смирнов", "Кузнецова", "Попов", "Васильева", "Соколов", "Морозова"]
    return f"{random.choice(first_names)} {random.choice(last_names)}"

def random_comment():
    comments = [
        "Пожалуйста, без опозданий",
        "Можно позвонить за 10 минут?",
        "Аллергия на латекс",
        "Приду с подругой",
        "",
        "Нужен чек",
        "Лучше не звонить до 12:00"
    ]
    return random.choice(comments)

# Основная функция
async def create_random_order():
    payload = {
        "master_id": random.randint(1, 5),          # Предполагаем, что мастера с ID 1–5 существуют
        "service_ids": random.sample(range(1, 21), k=random.randint(1, 3)),  # Услуги с ID 1–20
        "begin_date": random_date().isoformat(),
        "begin_time": random_time(),
        "customer_phone": random_phone(),
        "customer_name": random_name(),
        "customer_notice": random_comment() or None
    }

    print("Отправляем данные:", payload)

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(ORDER_URL, json=payload) as response:
                print(f"Статус ответа: {response.status}")
                try:
                    result = await response.json()
                    print("Ответ (JSON):", result)
                except Exception:
                    text = await response.text()
                    print("Ответ (текст):", text[:500])  # Ограничим вывод
        except aiohttp.ClientError as e:
            print(f"Ошибка подключения: {e}")

if __name__ == "__main__":
    asyncio.run(create_random_order())