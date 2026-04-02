"""
Тестирование YandexGeocoder.
"""

import os
from dotenv import load_dotenv

load_dotenv()
from geocoder import YandexGeocoder

# Вставьте ваш реальный API-ключ (получить можно на https://yandex.ru/dev/maps/geocoder/)
API_KEY = os.getenv("yandex_geo_key")
DSN = os.getenv("DATABASE_URL")

if __name__ == "__main__":

    with YandexGeocoder(API_KEY, DSN, use_cache=True) as geocoder:
        addresses = [
            "Москва, Широкая улица, 1А",
            "Москва, Широкая улица, 1А",  # повтор – возьмётся из кэша
            "Санкт-Петербург, Невский проспект, 1",
        ]
        for addr in addresses:
            coords = geocoder.geocode(addr)
            if coords:
                print(f"{addr}: {coords[0]:.6f}, {coords[1]:.6f}")
            else:
                print(f"{addr}: не найден")

        print(f"Размер кэша: {geocoder.get_cache_size()}")
