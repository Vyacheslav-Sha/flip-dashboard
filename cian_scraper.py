#!/usr/bin/env python3
"""
FLIP-проект: Парсер ЦИАН через Selenium (обход Cloudflare).

Собирает данные о квартирах на продажу в Москве для оценки
рыночной стоимости перед покупкой на банкротных торгах.

Примеры запуска:
    python3 cian_scraper.py
    python3 cian_scraper.py --rooms 2 --pages 2
    python3 cian_scraper.py --rooms 2 --min-price 15000000 --max-price 25000000
    python3 cian_scraper.py --rooms 2 --address "Балаклавский" --auction-price 15300000
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import random
from datetime import datetime

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
except ImportError:
    print("Установите: pip3 install selenium undetected-chromedriver")
    sys.exit(1)

from bs4 import BeautifulSoup


def parse_args():
    p = argparse.ArgumentParser(description="FLIP: Парсер квартир ЦИАН")
    p.add_argument("--rooms", type=str, default="2",
                   help="Комнаты: 1, 2, 3, studio, all (по умолч. 2)")
    p.add_argument("--pages", type=int, default=2, help="Страниц (по умолч. 2)")
    p.add_argument("--min-price", type=int, default=None, help="Мин. цена (руб)")
    p.add_argument("--max-price", type=int, default=None, help="Макс. цена (руб)")
    p.add_argument("--address", type=str, default=None, help="Фильтр по улице/адресу")
    p.add_argument("--district", type=str, default=None, help="Фильтр по району")
    p.add_argument("--auction-price", type=int, default=None, help="Цена на торгах для расчёта прибыли")
    p.add_argument("--renovation-cost", type=int, default=None, help="Бюджет ремонта")
    p.add_argument("--area", type=float, default=None, help="Площадь объекта (м²) для flip-расчёта")
    p.add_argument("--headless", action="store_true", help="Без окна браузера")
    p.add_argument("--output", type=str, default=None, help="Имя CSV-файла")
    return p.parse_args()


def build_url(page, rooms, min_price, max_price):
    params = {
        "engine_version": "2",
        "p": str(page),
        "region": "1",
        "deal_type": "sale",
        "offer_type": "flat",
        "with_neighbors": "0",
        "flat_share": "2",
        "sort": "creation_date_desc",
    }
    room_map = {"studio": "room9", "1": "room1", "2": "room2",
                "3": "room3", "4": "room4", "5": "room5"}
    if rooms != "all":
        for r in rooms.split(","):
            r = r.strip()
            params[room_map.get(r, f"room{r}")] = "1"
    if min_price:
        params["minprice"] = str(min_price)
    if max_price:
        params["maxprice"] = str(max_price)
    return "https://cian.ru/cat.php?" + "&".join(f"{k}={v}" for k, v in params.items())


def create_driver(headless=False):
    options = uc.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=ru-RU,ru")
    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(30)
    return driver


def parse_title(title_text):
    """Извлечь комнаты, площадь, этаж из заголовка типа '2-комн. квартира, 45,4 м², 23/25 этаж'."""
    rooms = 0
    meters = 0.0
    floor = 0
    floors_count = 0

    r = re.search(r"(\d)-комн", title_text)
    if r:
        rooms = int(r.group(1))
    elif "студия" in title_text.lower():
        rooms = 0

    m = re.search(r"([\d,\.]+)\s*м²", title_text)
    if m:
        meters = float(m.group(1).replace(",", "."))

    f = re.search(r"(\d+)/(\d+)\s*этаж", title_text)
    if f:
        floor = int(f.group(1))
        floors_count = int(f.group(2))

    return rooms, meters, floor, floors_count


def parse_price(price_text):
    """Извлечь цену из текста типа '15 000 000 ₽'."""
    cleaned = re.sub(r"[^\d]", "", price_text)
    return int(cleaned) if cleaned else 0


def parse_listing_page(soup):
    """Извлечь все объявления со страницы."""
    cards = soup.find_all(attrs={"data-name": "CardComponent"})
    listings = []

    for card in cards:
        listing = extract_card(card)
        if listing and listing.get("price", 0) > 0:
            listings.append(listing)

    return listings


def extract_card(card):
    """Извлечь данные из одной карточки объявления."""
    title_el = card.find(attrs={"data-name": "TitleComponent"})
    if not title_el:
        return None

    title_text = title_el.get_text(" ", strip=True)
    rooms, meters, floor, floors_count = parse_title(title_text)

    url = ""
    if title_el.name == "a":
        url = title_el.get("href", "")
    else:
        a = title_el.find("a", href=True)
        if a:
            url = a["href"]
    if url and not url.startswith("http"):
        url = "https://www.cian.ru" + url
    url = re.sub(r"\?context=.*$", "", url)

    price = 0
    price_m2 = 0
    price_section = None
    for row in card.find_all(attrs={"data-name": "GeneralInfoSectionRowComponent"}):
        row_text = row.get_text(" ", strip=True)
        if "₽" in row_text and re.search(r"\d", row_text):
            content_rows = row.find_all(attrs={"data-name": "ContentRow"})
            if len(content_rows) >= 1:
                price = parse_price(content_rows[0].get_text(strip=True))
            if len(content_rows) >= 2:
                pm2_text = content_rows[1].get_text(strip=True)
                price_m2 = parse_price(pm2_text.replace("₽/м²", ""))
            break

    if not price_m2 and price > 0 and meters > 0:
        price_m2 = int(price / meters)

    geo_labels = card.find_all(attrs={"data-name": "GeoLabel"})
    geo_parts = [g.get_text(strip=True) for g in geo_labels]

    city = ""
    district = ""
    metro = ""
    street = ""
    house_number = ""

    for part in geo_parts:
        if part in ("Москва", "Санкт-Петербург"):
            city = part
        elif part.startswith("м. "):
            metro = part.replace("м. ", "")
        elif part.startswith("р-н "):
            district = part.replace("р-н ", "")
        elif re.match(r"^\d", part) or re.match(r"^[А-Яа-я]\d", part):
            house_number = part
        elif any(w in part.lower() for w in
                 ["ул ", "ул.", "улица", "просп", "проспект", "бульв",
                  "переул", "шоссе", "набереж", "проезд", "аллея"]):
            street = part
        elif not district and "район" not in part.lower() and "округ" not in part.lower():
            if not street:
                street = part
            elif not district:
                district = part

    rc_el = card.find(attrs={"data-name": "ContentRow"})
    residential_complex = ""
    if rc_el:
        rc_text = rc_el.get_text(strip=True)
        jk = re.search(r"ЖК\s*[«\"](.+?)[»\"]", rc_text)
        if jk:
            residential_complex = jk.group(1)

    special_geo = card.find(attrs={"data-name": "SpecialGeo"})
    metro_time = ""
    if special_geo:
        mt = re.search(r"(\d+)\s*минут", special_geo.get_text(strip=True))
        if mt:
            metro_time = mt.group(0)
        if not metro:
            sg_text = special_geo.get_text(" ", strip=True)
            parts = sg_text.split()
            if parts:
                metro = parts[0]

    author = ""
    author_type = ""
    brand = card.find(attrs={"data-name": "BrandingLevelWrapper"})
    if brand:
        brand_text = brand.get_text(" ", strip=True)
        if "Собственник" in brand_text:
            author_type = "собственник"
        elif "Агентство" in brand_text or "агент" in brand_text.lower():
            author_type = "агентство"
        elif "Риелтор" in brand_text or "риелтор" in brand_text.lower():
            author_type = "риелтор"

    return {
        "price": price,
        "price_m2": price_m2,
        "rooms_count": rooms,
        "total_meters": meters,
        "floor": floor,
        "floors_count": floors_count,
        "city": city,
        "district": district,
        "metro": metro,
        "metro_time": metro_time,
        "street": street,
        "house_number": house_number,
        "residential_complex": residential_complex,
        "author_type": author_type,
        "url": url,
    }


def format_price(price):
    if price >= 1_000_000:
        return f"{price / 1_000_000:.2f} млн ₽"
    if price >= 1_000:
        return f"{price / 1_000:.0f} тыс ₽"
    return f"{price:,.0f} ₽".replace(",", " ")


def filter_results(data, address=None, district=None):
    result = data
    if address:
        q = address.lower()
        result = [
            d for d in result
            if q in d.get("street", "").lower()
            or q in d.get("house_number", "").lower()
            or q in d.get("district", "").lower()
            or q in d.get("residential_complex", "").lower()
            or q in d.get("metro", "").lower()
        ]
    if district:
        q = district.lower()
        result = [d for d in result if q in d.get("district", "").lower()]
    return result


def print_analysis(data, title):
    if not data:
        print(f"\n  Нет данных для «{title}»")
        return None

    prices = [d["price"] for d in data if d["price"] > 0]
    meters = [d["total_meters"] for d in data if d["total_meters"] > 0]
    pm2 = [d["price"] / d["total_meters"] for d in data
           if d["price"] > 0 and d["total_meters"] > 0]

    if not prices:
        print(f"\n  Нет объявлений с ценами ({title})")
        return None

    avg_p = sum(prices) / len(prices)
    med_p = sorted(prices)[len(prices) // 2]
    avg_m = sum(meters) / len(meters) if meters else 0
    avg_pm2 = sum(pm2) / len(pm2) if pm2 else 0
    med_pm2 = sorted(pm2)[len(pm2) // 2] if pm2 else 0

    print(f"\n{'=' * 64}")
    print(f"  АНАЛИЗ РЫНКА: {title}")
    print(f"{'=' * 64}")
    print(f"  Объявлений:          {len(data)}")
    print(f"  Средняя площадь:     {avg_m:.1f} м²")
    print(f"{'─' * 64}")
    print(f"  ЦЕНЫ:")
    print(f"  Средняя:             {format_price(int(avg_p))}")
    print(f"  Медианная:           {format_price(int(med_p))}")
    print(f"  Мин:                 {format_price(min(prices))}")
    print(f"  Макс:                {format_price(max(prices))}")
    print(f"{'─' * 64}")
    print(f"  ЦЕНА ЗА М²:")
    print(f"  Средняя:             {format_price(int(avg_pm2))}")
    print(f"  Медианная:           {format_price(int(med_pm2))}")
    print(f"{'=' * 64}")

    sorted_d = sorted(data, key=lambda x: x["price"])
    print(f"\n  ТОП-10 САМЫХ ДЕШЁВЫХ:")
    print(f"  {'─' * 60}")
    for i, it in enumerate(sorted_d[:10], 1):
        m2 = it["total_meters"]
        p = it["price"]
        r = it["rooms_count"]
        fl = it["floor"]
        fls = it["floors_count"]
        p_m2 = int(p / m2) if m2 > 0 else 0
        r_s = f"{r}-к" if r > 0 else "ст."

        print(f"  {i:2}. {format_price(p):>12} | {m2:5.1f} м² | {r_s:4} | "
              f"эт.{fl}/{fls} | {p_m2:>7,} ₽/м²".replace(",", " "))
        addr = f"{it['street']} {it['house_number']}".strip()
        metro = f"м. {it['metro']}" if it['metro'] else ""
        jk = f"ЖК {it['residential_complex']}" if it['residential_complex'] else ""
        details = " | ".join(x for x in [addr, metro, jk] if x)
        if details:
            print(f"      {details}")
        print(f"      {it['url']}")

    return {"avg_pm2": avg_pm2, "med_pm2": med_pm2, "avg_m": avg_m, "count": len(data)}


def calculate_flip(avg_pm2, area, auction_price, renovation_cost=None):
    market_price = int(avg_pm2 * area)
    if renovation_cost is None:
        renovation_cost = int(area * 45_000)

    total_invest = auction_price + renovation_cost
    sale_reno = int(market_price * 1.15)
    sale_price = int(sale_reno * 0.95)
    gross = sale_price - total_invest

    deductible = auction_price + int(renovation_cost * 0.6)
    taxable = max(0, sale_price - deductible)
    tax = int(taxable * 0.05)
    net = gross - tax

    discount = (1 - auction_price / market_price) * 100

    print(f"\n{'=' * 64}")
    print(f"  FLIP-РАСЧЁТ СДЕЛКИ")
    print(f"{'=' * 64}")
    print(f"  Площадь объекта:          {area:.1f} м²")
    print(f"  Рыночная (без ремонта):   {format_price(market_price)}")
    print(f"  Цена на торгах:           {format_price(auction_price)}")
    print(f"  Дисконт от рынка:         {discount:.1f}%")
    print(f"{'─' * 64}")
    print(f"  Ремонт + матер. + мебель: {format_price(renovation_cost)}")
    print(f"  Себестоимость:            {format_price(total_invest)}")
    print(f"  Рыночная с ремонтом:      {format_price(sale_reno)}")
    print(f"  Цена продажи (−5% рынка): {format_price(sale_price)}")
    print(f"{'─' * 64}")
    print(f"  Валовая прибыль:          {format_price(gross)}")
    print(f"  Налог УСН 5%:             {format_price(tax)}")
    print(f"  Чистая прибыль:           {format_price(net)}")
    print(f"  На каждого (50/50):       {format_price(net // 2)}")
    if total_invest > 0:
        print(f"  ROI сделки:               {net / total_invest * 100:.1f}%")
        print(f"  ROI годовой (6 сд./год):  {net / total_invest * 100 * 6:.1f}%")
    v = "ВЫГОДНО" if net > 500_000 else "МАРЖА МАЛА" if net > 0 else "УБЫТОК"
    print(f"  Вердикт:                  {v}")
    print(f"{'=' * 64}")


def save_csv(data, filename):
    if not data:
        return
    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"\n  CSV: {filepath}")


def save_dashboard_json(data, json_path=None):
    """Сохранить JSON с данными и аналитикой для FLIP Dashboard."""
    if not data:
        return

    prices = [d["price"] for d in data if d["price"] > 0]
    meters = [d["total_meters"] for d in data if d["total_meters"] > 0]
    pm2 = [d["price"] / d["total_meters"] for d in data
           if d["price"] > 0 and d["total_meters"] > 0]

    if not prices or not pm2:
        return

    by_district = {}
    for d in data:
        dist = d.get("district") or d.get("metro") or "Другое"
        if dist not in by_district:
            by_district[dist] = []
        if d["price"] > 0 and d["total_meters"] > 0:
            by_district[dist].append(d["price"] / d["total_meters"])

    district_stats = {}
    for dist, vals in by_district.items():
        if vals:
            district_stats[dist] = {
                "avg_pm2": int(sum(vals) / len(vals)),
                "med_pm2": int(sorted(vals)[len(vals) // 2]),
                "count": len(vals),
            }

    dashboard_data = {
        "meta": {
            "date": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "source": "cian.ru",
            "total_listings": len(data),
            "location": "Москва",
        },
        "market": {
            "avg_price": int(sum(prices) / len(prices)),
            "med_price": int(sorted(prices)[len(prices) // 2]),
            "min_price": min(prices),
            "max_price": max(prices),
            "avg_meters": round(sum(meters) / len(meters), 1) if meters else 0,
            "avg_price_m2": int(sum(pm2) / len(pm2)),
            "med_price_m2": int(sorted(pm2)[len(pm2) // 2]),
            "price_m2_with_reno": int(sum(pm2) / len(pm2) * 1.22),
        },
        "districts": district_stats,
        "listings": [
            {
                "price": d["price"],
                "price_m2": d["price_m2"],
                "rooms": d["rooms_count"],
                "meters": d["total_meters"],
                "floor": d["floor"],
                "floors_count": d["floors_count"],
                "district": d.get("district", ""),
                "metro": d.get("metro", ""),
                "metro_time": d.get("metro_time", ""),
                "street": d.get("street", ""),
                "house": d.get("house_number", ""),
                "jk": d.get("residential_complex", ""),
                "author": d.get("author_type", ""),
                "url": d.get("url", ""),
            }
            for d in data if d["price"] > 0
        ],
    }

    if not json_path:
        json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cian_data.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(dashboard_data, f, ensure_ascii=False, indent=2)
    print(f"  JSON: {json_path}")


def main():
    args = parse_args()

    print(f"\n{'=' * 64}")
    print(f"  FLIP-ПРОЕКТ: Парсер ЦИАН (Selenium)")
    print(f"  {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"  Комнаты: {args.rooms} | Страницы: {args.pages}")
    if args.min_price or args.max_price:
        lo = format_price(args.min_price) if args.min_price else "—"
        hi = format_price(args.max_price) if args.max_price else "—"
        print(f"  Цена: {lo} — {hi}")
    if args.address:
        print(f"  Фильтр адрес: {args.address}")
    print(f"{'=' * 64}")

    print("\n  Запуск браузера...")
    driver = create_driver(headless=args.headless)
    all_listings = []

    try:
        for page in range(1, args.pages + 1):
            url = build_url(page, args.rooms, args.min_price, args.max_price)
            print(f"\n  Страница {page}/{args.pages}: загрузка...")

            try:
                driver.get(url)
            except Exception as e:
                print(f"  Ошибка загрузки: {e}")
                print("  Перезапуск браузера...")
                try:
                    driver.quit()
                except Exception:
                    pass
                time.sleep(2)
                driver = create_driver(headless=args.headless)
                driver.get(url)

            time.sleep(random.uniform(5, 8))

            for attempt in range(3):
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, "[data-name='CardComponent']"))
                    )
                    break
                except Exception:
                    if attempt < 2:
                        print(f"  Контент не найден, попытка {attempt + 2}...")
                        driver.refresh()
                        time.sleep(random.uniform(4, 6))
                    else:
                        print(f"  Страница {page}: контент не загрузился, пропуск")

            for _ in range(4):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.5)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            listings = parse_listing_page(soup)
            all_listings.extend(listings)
            print(f"  Страница {page}: {len(listings)} объявлений (всего: {len(all_listings)})")

            if page < args.pages:
                delay = random.uniform(4, 7)
                print(f"  Пауза {delay:.1f} сек...")
                time.sleep(delay)

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        print("\n  Браузер закрыт.")

    if not all_listings:
        print("\n  Не удалось собрать данные. Попробуйте без --headless.")
        return

    working = all_listings
    if args.address or args.district:
        filtered = filter_results(all_listings, args.address, args.district)
        label = args.address or args.district
        stats = print_analysis(filtered, f"Фильтр: {label}")
        if len(filtered) < len(all_listings):
            print_analysis(all_listings, "Вся выборка")
        working = filtered if filtered else all_listings
    else:
        stats = print_analysis(all_listings, "Москва — 2-комн.")

    if args.auction_price and stats:
        area = args.area or stats["avg_m"]
        calculate_flip(
            stats["avg_pm2"], area,
            args.auction_price, args.renovation_cost
        )

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    filename = args.output or f"cian_{args.rooms}комн_{ts}.csv"
    save_csv(all_listings, filename)
    save_dashboard_json(all_listings)


if __name__ == "__main__":
    main()
