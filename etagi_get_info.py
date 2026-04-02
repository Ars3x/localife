from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
import json
import csv
import time
import random
import os
import re
from typing import List, Dict, Optional
import math

# ------------------- Настройки браузера -------------------
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
options = webdriver.ChromeOptions()
options.add_argument(f"user-agent={user_agent}")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--disable-extensions")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

options.add_argument(
    "--headless"
)  # <-- ЭТО ГЛАВНАЯ СТРОКА, которая включает headless-режим[reference:1]
# Опционально: отключаем GPU, ускоряет работу в headless-режиме
options.add_argument("--disable-gpu")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)


# ------------------- Функции -------------------
def get_total_pages_from_count(driver) -> Optional[int]:
    """Определяет общее количество страниц по элементу с количеством предложений."""
    try:
        count_elem = driver.find_element(By.CSS_SELECTOR, "div.s2IqK")
        text = count_elem.text
        numbers = re.findall(r"[\d\s]+", text)
        if numbers:
            raw = "".join(numbers[0].split())
            total_offers = int(raw)
            pages = math.ceil(total_offers / 30)
            return pages
    except Exception:
        pass
    return None


def get_last_page_from_paginator(driver) -> Optional[int]:
    """Извлекает максимальный номер страницы из пагинатора на текущей странице."""
    try:
        # Ищем все кнопки, содержащие только цифры (номера страниц)
        page_buttons = driver.find_elements(By.CSS_SELECTOR, "button.plr_B24.plr_gu8")
        page_numbers = []
        for btn in page_buttons:
            text = btn.text.strip()
            if text.isdigit():
                page_numbers.append(int(text))
        if page_numbers:
            return max(page_numbers)
    except Exception:
        pass
    # Если не нашли, пробуем альтернативный селектор (на всякий случай)
    try:
        paginator = driver.find_element(By.CSS_SELECTOR, "nav.plr_Wxz")
        nums = re.findall(r"\b\d+\b", paginator.text)
        nums = [int(n) for n in nums if int(n) > 0]
        if nums:
            return max(nums)
    except Exception:
        pass
    return None


def extract_json_from_script(html: str) -> Optional[str]:
    """Извлекает JSON-объект из переменной var data= в HTML."""
    start_marker = "var data="
    start_pos = html.find(start_marker)
    if start_pos == -1:
        return None
    after_marker = start_pos + len(start_marker)
    brace_start = html.find("{", after_marker)
    if brace_start == -1:
        return None

    balance = 0
    in_string = False
    escape = False
    for i in range(brace_start, len(html)):
        ch = html[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
        else:
            if ch == '"':
                in_string = True
            elif ch == "{":
                balance += 1
            elif ch == "}":
                balance -= 1
                if balance == 0:
                    return html[brace_start : i + 1]
    return None


def parse_page(html: str) -> List[Dict]:
    """Парсит HTML страницы и возвращает список объявлений."""
    data_json_str = extract_json_from_script(html)
    if not data_json_str:
        return []

    try:
        data = json.loads(data_json_str)
    except json.JSONDecodeError:
        return []

    flats = data.get("lists", {}).get("flats", [])
    if not flats:
        return []

    keep_dict = {item["value"]: item["label"] for item in data.get("flatsKeepList", [])}

    parsed = []
    for flat in flats:
        ticket_id = flat.get("_ticket_id")
        price = flat.get("price")
        square = flat.get("square")
        rooms = flat.get("rooms")
        studio = flat.get("studio", False)
        floor = flat.get("floor")
        floors_total = flat.get("floors")
        building_year = flat.get("building_year")
        metro = flat.get("metro_name")
        metro_dist = flat.get("metro_distance")
        time_to_metro = flat.get("time_to_metro")
        main_photo = flat.get("main_photo")
        keep_code = flat.get("keep")
        keep_label = keep_dict.get(keep_code, "Не указано")

        # Адрес
        meta = flat.get("meta", {})
        address_parts = [
            meta.get("city", ""),
            meta.get("district", ""),
            f"{meta.get('street', '')} {flat.get('house_address_number', '')}".strip(),
        ]
        address = ", ".join(filter(None, address_parts)).strip(", ")
        if not address:
            address = "Адрес не указан"

        url_offer = f"https://msk.etagi.com/realty/{ticket_id}/" if ticket_id else ""

        parsed.append(
            {
                "ticket_id": ticket_id,
                "price": price,
                "square": square,
                "rooms": rooms,
                "studio": studio,  # является ли студией
                "floor": floor,
                "floors_total": floors_total,
                "building_year": building_year,
                "metro": metro,
                "metro_dist": metro_dist,
                "time_to_metro": time_to_metro,
                "address": address,
                "keep": keep_label,
                "photo": main_photo,
                "url": url_offer,
            }
        )
    return parsed


def append_to_csv(flats: List[Dict], filename: str, first_run: bool = False):
    """Добавляет записи в CSV. Если first_run=True, файл перезаписывается (создаётся новый)."""
    keys = [
        "ticket_id",
        "price",
        "square",
        "rooms",
        "studio",
        "floor",
        "floors_total",
        "building_year",
        "metro",
        "metro_dist",
        "time_to_metro",
        "address",
        "keep",
        "photo",
        "url",
    ]
    mode = "w" if first_run else "a"
    with open(filename, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        if first_run:
            writer.writeheader()
        writer.writerows(flats)


def load_last_processed_page(progress_file: str) -> int:
    """Читает номер последней обработанной страницы из файла. Если файла нет, возвращает 0."""
    if os.path.exists(progress_file):
        with open(progress_file, "r", encoding="utf-8") as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return 0
    return 0


def save_last_processed_page(progress_file: str, page: int):
    """Сохраняет номер последней обработанной страницы в файл."""
    with open(progress_file, "w", encoding="utf-8") as f:
        f.write(str(page))


def main():
    csv_filename = "etagi_flats.csv"
    progress_file = "last_page.txt"

    # Определяем, нужно ли создавать заголовок (если файла нет)
    first_run = not os.path.exists(csv_filename)

    # 1. Загружаем первую страницу, чтобы получить общее количество страниц
    print("Загрузка первой страницы...")
    driver.get("https://msk.etagi.com/realty/?orderId=datecreatedesc")
    try:
        WebDriverWait(driver, 15).until(lambda d: "var data=" in d.page_source)
    except TimeoutException:
        print("Не удалось загрузить данные на первой странице")
        driver.quit()
        return

    # Получаем количество страниц
    total_pages = get_total_pages_from_count(driver)
    if not total_pages:
        total_pages = get_last_page_from_paginator(driver)
    if not total_pages:
        print("Не удалось определить количество страниц. Завершаем.")
        driver.quit()
        return
    print(f"Всего страниц: {total_pages}")

    # Загружаем последнюю обработанную страницу из файла
    last_processed = load_last_processed_page(progress_file)
    start_page = last_processed + 1 if last_processed < total_pages else total_pages + 1

    if start_page > total_pages:
        print("Все страницы уже обработаны. Новых страниц нет.")
        driver.quit()
        return

    print(f"Начинаем со страницы {start_page}")

    # 2. Обрабатываем страницы с start_page до total_pages
    for page_num in range(start_page, total_pages + 1):
        # Если это не первая страница (или если мы не на ней), переходим
        if page_num != 1 or page_num != start_page:
            url = f"https://msk.etagi.com/realty/?page={page_num}"
            print(f"Переход на страницу {page_num}...")
            driver.get(url)
            try:
                WebDriverWait(driver, 15).until(lambda d: "var data=" in d.page_source)
            except TimeoutException:
                print(f"  Не удалось загрузить страницу {page_num}, пропускаем.")
                continue

        print(f"Парсинг страницы {page_num}...")
        html = driver.page_source
        flats = parse_page(html)
        if flats:
            print(f"  Найдено объявлений: {len(flats)}")
            # При первом запуске (first_run) и на первой странице создаём заголовок
            if page_num == start_page and first_run:
                append_to_csv(flats, csv_filename, first_run=True)
                first_run = False
            else:
                append_to_csv(flats, csv_filename, first_run=False)
        else:
            print("  Объявления не найдены")

        # Сохраняем прогресс после каждой успешно обработанной страницы
        # (записываем номер обработанной страницы, а не следующую)
        save_last_processed_page(progress_file, page_num)

        # Задержка между страницами (кроме последней)
        if page_num < total_pages:
            delay = random.uniform(2, 9)
            print(f"  Ожидание {delay:.1f} секунд...")
            time.sleep(delay)

    print(f"\nГотово! Все страницы до {total_pages} обработаны.")
    driver.quit()


if __name__ == "__main__":
    main()
