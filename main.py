"""
Тестирование YandexGeocoder.
"""
import os
from dotenv import load_dotenv
load_dotenv()
from geocoder import YandexGeocoder

# Вставьте ваш реальный API-ключ (получить можно на https://yandex.ru/dev/maps/geocoder/)
API_KEY = os.getenv("yandex_geo_key")

if __name__ == "__main__":
    geocoder = YandexGeocoder(API_KEY)

    addresses = [
        "Москва, Широкая улица, 1А",
        "Россия, Москва, ул. Широкая, 1А",
        "Санкт-Петербург, Невский проспект, 1",
        "несуществующий адрес 123456",
    ]

    for addr in addresses:
        coords = geocoder.geocode(addr)
        if coords:
            print(f"{addr} -> {coords[0]:.6f}, {coords[1]:.6f}")
        else:
            print(f"{addr} -> не найден")