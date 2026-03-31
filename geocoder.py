"""
Модуль для геокодирования адресов через API Яндекс.Карт.
"""

import requests
from typing import Optional, Tuple, Dict, Any


class YandexGeocoder:
    """
    Геокодер на основе API Яндекс.Карт.
    """

    # Базовый URL для геокодирования
    GEOCODE_URL = "https://geocode-maps.yandex.ru/1.x/"

    def __init__(self, api_key: str):
        """
        Инициализация геокодера с API-ключом.

        :param api_key: API-ключ Яндекс.Карт (можно получить на yandex.ru/dev/maps)
        """
        self.api_key = api_key
        self.session = requests.Session()  # переиспользуем сессию для эффективности

    def geocode(self, address: str) -> Optional[Tuple[float, float]]:
        """
        Преобразует адрес в координаты (долгота, широта).

        :param address: адрес в свободной форме (например, "Москва, Широкая улица, 1А")
        :return: кортеж (lon, lat) или None, если адрес не найден/ошибка
        """
        params = {
            "apikey": self.api_key,
            "geocode": address,
            "format": "json",
        }

        try:
            response = self.session.get(self.GEOCODE_URL, params=params)
            response.raise_for_status()  # выбросит исключение при HTTP-ошибке

            data = response.json()
            return self._extract_coordinates(data)

        except requests.exceptions.RequestException as e:
            print(f"Ошибка HTTP-запроса: {e}")
            return None
        except ValueError as e:
            print(f"Ошибка парсинга JSON: {e}")
            return None
        except Exception as e:
            print(f"Неожиданная ошибка: {e}")
            return None

    def _extract_coordinates(self, data: Dict[str, Any]) -> Optional[Tuple[float, float]]:
        """
        Извлекает координаты из ответа API.

        :param data: JSON-ответ от сервера
        :return: (lon, lat) или None
        """
        try:
            # Навигация по структуре JSON:
            # response.GeoObjectCollection.featureMember[0].GeoObject.Point.pos
            feature_members = data["response"]["GeoObjectCollection"]["featureMember"]
            if not feature_members:
                return None

            point_str = feature_members[0]["GeoObject"]["Point"]["pos"]
            # Координаты приходят как "долгота широта"
            lon_str, lat_str = point_str.split()
            return float(lon_str), float(lat_str)

        except (KeyError, IndexError, ValueError) as e:
            print(f"Ошибка извлечения координат: {e}")
            return None