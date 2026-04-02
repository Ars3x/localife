"""
Модуль для геокодирования адресов через API Яндекс.Карт с кэшированием в PostgreSQL.
"""

import time
import requests
import psycopg2
from psycopg2 import sql, extras
from typing import Optional, Tuple, List, Dict, Any
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


class YandexGeocoder:
    """
    Геокодер на основе API Яндекс.Карт с кэшированием результатов в PostgreSQL.
    """

    GEOCODE_URL = "https://geocode-maps.yandex.ru/v1/"
    DEFAULT_BBOX = (35.5, 54.5, 39.0, 56.5)  # Москва и Московская область

    def __init__(
        self,
        api_key: str,
        dsn: str,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        use_cache: bool = True,
    ):
        """
        Инициализация геокодера.

        :param api_key: API-ключ Яндекс.Карт
        :param dsn: строка подключения к PostgreSQL (например, "host=localhost dbname=... user=... password=...")
        :param bbox: ограничивающий прямоугольник (lon_min, lat_min, lon_max, lat_max)
        :param use_cache: использовать ли кэш в БД
        """
        self.api_key = api_key
        self.bbox = bbox or self.DEFAULT_BBOX
        self.use_cache = use_cache

        # Настройка сессии с повторными попытками
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)

        # Подключение к PostgreSQL
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = False  # управляем транзакциями вручную
        self._init_db()

    def _init_db(self) -> None:
        """Создаёт таблицу кэша, если её нет."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS geocoder_cache (
                    address TEXT PRIMARY KEY,
                    lon DOUBLE PRECISION,
                    lat DOUBLE PRECISION,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """
            )
            self.conn.commit()

    def _normalize_address(self, address: str) -> str:
        """Приводит адрес к каноническому виду для ключа кэша."""
        return " ".join(address.strip().lower().split())

    def _get_from_db(self, norm_addr: str) -> Optional[Tuple[float, float]]:
        """Возвращает координаты из кэша или None."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT lon, lat FROM geocoder_cache WHERE address = %s", (norm_addr,)
            )
            row = cur.fetchone()
            if row and row[0] is not None and row[1] is not None:
                return (row[0], row[1])
            return None

    def _save_to_db(
        self, norm_addr: str, coords: Optional[Tuple[float, float]]
    ) -> None:
        """Сохраняет результат в кэш."""
        lon = coords[0] if coords else None
        lat = coords[1] if coords else None
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO geocoder_cache (address, lon, lat)
                VALUES (%s, %s, %s)
                ON CONFLICT (address) DO UPDATE SET
                    lon = EXCLUDED.lon,
                    lat = EXCLUDED.lat,
                    created_at = NOW()
            """,
                (norm_addr, lon, lat),
            )
            self.conn.commit()

    def geocode(self, address: str) -> Optional[Tuple[float, float]]:
        """
        Преобразует адрес в координаты (долгота, широта).

        Сначала проверяет кэш в БД, при промахе делает запрос к API.
        Результат сохраняется в БД (включая None для отсутствующих адресов).

        :param address: адрес в свободной форме
        :return: кортеж (lon, lat) или None, если адрес не найден/ошибка
        """
        norm_addr = self._normalize_address(address)

        if self.use_cache:
            coords = self._get_from_db(norm_addr)
            if coords is not None:
                return coords

        # Ограничиваем частоту запросов к API (примерно 5 запросов в секунду)
        time.sleep(0.2)

        params = {
            "apikey": self.api_key,
            "geocode": address,
            "format": "json",
            "bbox": f"{self.bbox[0]},{self.bbox[1]}~{self.bbox[2]},{self.bbox[3]}",
            "rspn": 1,
        }

        try:
            resp = self.session.get(self.GEOCODE_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            coords = self._extract_coordinates(data)
        except Exception as e:
            print(f"Ошибка геокодирования '{address}': {e}")
            coords = None

        if self.use_cache:
            self._save_to_db(norm_addr, coords)

        return coords

    def _extract_coordinates(
        self, data: Dict[str, Any]
    ) -> Optional[Tuple[float, float]]:
        """Извлекает координаты из ответа API."""
        try:
            members = data["response"]["GeoObjectCollection"]["featureMember"]
            if not members:
                return None
            point_str = members[0]["GeoObject"]["Point"]["pos"]
            lon_str, lat_str = point_str.split()
            return float(lon_str), float(lat_str)
        except (KeyError, IndexError, ValueError) as e:
            print(f"Ошибка извлечения координат: {e}")
            return None

    def set_search_area(self, bbox: Tuple[float, float, float, float]) -> None:
        """Устанавливает новую область поиска."""
        self.bbox = bbox

    def clear_cache(self) -> None:
        """Очищает всю таблицу кэша."""
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM geocoder_cache")
            self.conn.commit()

    def get_cache_size(self) -> int:
        """Возвращает количество записей в кэше."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM geocoder_cache")
            return cur.fetchone()[0]

    def close(self) -> None:
        """Закрывает соединение с БД."""
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
