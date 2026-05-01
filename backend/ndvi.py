"""
Модуль расчета индекса озеленения (NDVI) через Google Earth Engine
Зависимости: earthengine-api, certifi, python-dotenv.
"""


from __future__ import annotations # Позволяет использовать имя класса внутри самого себя (до того, как он полностью создан)
import os # Работа с файловой системой (чтение/запись, получение путей к файлам и каталогам).
import ssl # Работа с SSL-контекстом (доверенные корневые сертификаты).
import urllib.request # Низкоуровневые HTTP(S)-запросы; патчим urlopen для корректных сертификатов на macOS.
from typing import Any, Literal

import certifi # Актуальные сертификаты безопасности (файл cacert.pem)
import ee # Библиотека для работы со спутниками Google Earth Engine
from dotenv import load_dotenv # Загрузка настроек из файла .env
from datetime import datetime, timedelta

_DEFAULT_PROJECT = "localife-ndvi" # ID проекта в Google Cloud


# Функция для настройки SSL-сертификатов на macOS
def _setup_ssl_for_macos() -> None:
    ca = certifi.where() # Получаем путь к файлу cacert.pem
    os.environ["SSL_CERT_FILE"] = ca # Указание для OpenSSL
    os.environ["REQUESTS_CA_BUNDLE"] = ca # Указание для requests (если EE или зависимости ходят через неё).
    
    # Создаем свои настройки безопасности на базе сертификатов certifi
    ssl_context = ssl.create_default_context(cafile=ca)
    original_urlopen = urllib.request.urlopen
    
    # Переделываем стандартную функцию открытия ссылок, чтобы она всегда использовала нашу защиту
    def patched_urlopen(*args: Any, **kwargs: Any) -> Any:
        if "context" not in kwargs:
            kwargs["context"] = ssl_context
        return original_urlopen(*args, **kwargs)

    # Заменяем стандартный инструмент на наш "улучшенный" гы-гы-гы
    urllib.request.urlopen = patched_urlopen  # type: ignore # Вынужденная подмена для исправления SSL на macOS


# Основная функция для расчета NDVI
def calculate_ndvi(image: ee.Image) -> ee.Image:
    ndvi = image.normalizedDifference(["B8", "B4"]).rename("NDVI") # Вычисляем разницу между каналами B8 и B4 и называем результат "NDVI"
    return image.addBands(ndvi) # Приклеиваем полученный результат новой полосой к основному снимку


# Ограничиваем выбор режима обработки: либо "первый снимок", либо "среднее по всем"
CompositeMode = Literal["first", "median"]


# Функция для расчета NDVI в конкретной точке
def get_location_score(
    lat: float,
    lon: float,
    *,
    buffer_m: int = 500,
    scale_m: int = 10, # для спутника Sentinel-2 максимальное разрешение это 10 метров
    max_cloud_pct: float | None = 60.0, # Максимально допустимая облачность в процентах
    collection_id: str = "COPERNICUS/S2_SR_HARMONIZED",
) -> float:
    
    # Определяем даты для запроса данных
    now = datetime.now()
    year = now.year
    month = now.month
    if month < 6: # Если сейчас еще не наступило лето текущего года, берем июль предыдущего года
        start_date = f"{year-1}-07-01"
        end_date = f"{year-1}-07-31"
    elif month > 8: # Если лето уже прошло, берем июль текущего года
        start_date = f"{year}-07-01"
        end_date = f"{year}-08-31"
    else:
        # Если сейчас лето (июнь-август), берем данные за последние 30 дней
        start_date = (now - timedelta(days=30)).strftime('%Y-%m-%d') # Берем данные за последние 30 дней
        end_date = now.strftime('%Y-%m-%d') # Берем данные за текущий день
    
    print(f"Анализируем период: {start_date} - {end_date}") # Выводим информацию о периоде

    point = ee.Geometry.Point([lon, lat]) # Создаем объект ee.Geometry.Point - точку в координатах WGS84
    
    # Поиск снимков в коллекции по заданной точке и периоду. Возвращает объект ee.ImageCollection (пачка снимков за указанный период)
    coll = ee.ImageCollection(collection_id).filterBounds(point).filterDate(start_date, end_date)
   
    # Фильтр облачности 
    # Просматривает метаданные каждого снимка в «пачке» и выбрасывает те, где облачность выше порога (например, 60%)
    # Возвращает объект ee.ImageCollection с оставшимися снимками
    if max_cloud_pct is not None:
        coll = coll.filter(ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", max_cloud_pct))

    # Подсчет количества снимков в коллекции.
    # coll.size() возвращает объект типа ee.Number. Вычисления происходят на серверах GEE
    # getInfo() обращается на сервер GEE и получает число или None
    n = int(coll.size().getInfo()) # Получаем количество снимков в коллекции
    if n == 0: # Если количество снимков равно 0, выбрасываем ошибку
        raise ValueError(f"Не удалось найти чистых снимков за период {start_date} - {end_date}")
    
    # 5. Выбор лучшего снимка (Метод 'first')
    # Сортируем по облачности (в начале самые чистые) и берем один лучший кадр.
    # Это дает максимально четкую и реалистичную картину состояния экологии.
    best_image = coll.sort("CLOUDY_PIXEL_PERCENTAGE").first()
    
    # Считаем индекс NDVI для выбранного снимка
    final_image = calculate_ndvi(best_image).select("NDVI")

    # Считаем среднее значение по региону, переменная stats
    stats: dict[str, Any] = (
        final_image.reduceRegion(
            reducer=ee.Reducer.mean(), # Выбор метода вычисления NDVI для всех точек в радиусе
            geometry=point.buffer(buffer_m), # Радиус круга в метрах вокруг точки
            scale=scale_m, # Разрешение в метрах
        )
        .getInfo() # Запрос в Google Earth Engine и возвращает результат в виде словаря (объект ee.Dictionary)
    )
    
    raw = stats.get("NDVI") # Достает число из словаря
    if raw is None: # проверяет, если оно пустое
        raise ValueError( # Выдает сообщение
            f"Нет данных NDVI в радиусе {buffer_m}м. Возможно, там вода или край снимка."
        )
        
    return round(float(raw), 3) # Если нет возвращает стандартное дробное число


# Подготовка библиотеки Google Earth Engine к работе
# загружает настройки, чинит SSL и авторизует проект
def init_ee(project: str | None = None, key_path: str | None = None) -> None:
    load_dotenv()
    # _setup_ssl_for_macos()  # не нужно на Linux
    
    if key_path and os.path.exists(key_path):
        # Используйте переменную окружения или явно укажите email аккаунта
        sa_email = 'ndvi-112@localife-ndvi.iam.gserviceaccount.com'
        credentials = ee.ServiceAccountCredentials(sa_email, key_path)
        proj = _DEFAULT_PROJECT
        ee.Initialize(credentials, project=proj)
    else:
        proj = _DEFAULT_PROJECT
        ee.Initialize(project=proj)


# Точка входа при запуске файла как скрипта: python NDVI.py (не выполняется при import).
if __name__ == "__main__":
    init_ee() # Подключаемся к EE с учётом .env и SSL.
    lat, lon = 58.693560, 56.064610 # 2. Координаты объекта недвижимости
    score = get_location_score(lat, lon) # Вызываем функцию вычисления NDVI
    print(f"зеленый индекс района: {score:.3f}")
