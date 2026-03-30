#!/usr/bin/env python3
"""
FLIP-проект: Анализатор рынка недвижимости через ЦИАН.

Быстрая оценка рыночной стоимости квартиры для принятия решения
о покупке на банкротных торгах.

Использование:
    python3 cian_analyzer.py                          # интерактивный режим
    python3 cian_analyzer.py --address "Балаклавский"  # по адресу
    python3 cian_analyzer.py --rooms 2 --min-price 10000000 --max-price 25000000
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime

try:
    import cianparser
except ImportError:
    print("Установите библиотеку: pip3 install cianparser")
    sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(description="FLIP: Анализ цен на квартиры в Москве через ЦИАН")
    parser.add_argument("--location", default="Москва", help="Город (по умолч. Москва)")
    parser.add_argument("--rooms", type=str, default=None,
                        help="Кол-во комнат: 1, 2, 3 или studio. Через запятую для нескольких: 1,2")
    parser.add_argument("--min-price", type=int, default=None, help="Мин. цена (руб)")
    parser.add_argument("--max-price", type=int, default=None, help="Макс. цена (руб)")
    parser.add_argument("--pages", type=int, default=3, help="Кол-во страниц для парсинга (по умолч. 3)")
    parser.add_argument("--address", type=str, default=None,
                        help="Фильтр по улице/адресу (поиск в результатах)")
    parser.add_argument("--district", type=str, default=None, help="Фильтр по району")
    parser.add_argument("--metro", type=str, default=None, help="Станция метро")
    parser.add_argument("--min-floor", type=int, default=None)
    parser.add_argument("--max-floor", type=int, default=None)
    parser.add_argument("--house-type", type=int, default=None,
                        help="Тип дома: 1=кирпич, 2=монолит, 3=панель, 8=кирпично-монолитный")
    parser.add_argument("--min-year", type=int, default=None, help="Год постройки от")
    parser.add_argument("--extra-data", action="store_true",
                        help="Доп. данные (медленнее в 5-10 раз, но год постройки, тип дома и т.д.)")
    parser.add_argument("--save-csv", action="store_true", help="Сохранить результаты в CSV")
    parser.add_argument("--no-shares", action="store_true", help="Исключить доли")
    parser.add_argument("--owner-only", action="store_true", help="Только от собственников")
    parser.add_argument("--auction-price", type=int, default=None,
                        help="Цена на торгах — рассчитает прибыльность сделки")
    parser.add_argument("--renovation-cost", type=int, default=None,
                        help="Бюджет ремонта (по умолч. рассчитывается автоматически)")
    return parser.parse_args()


def format_price(price):
    if price >= 1_000_000:
        return f"{price / 1_000_000:.2f} млн ₽"
    return f"{price:,.0f} ₽".replace(",", " ")


def build_settings(args):
    settings = {
        "start_page": 1,
        "end_page": args.pages,
    }

    if args.min_price:
        settings["min_price"] = args.min_price
    if args.max_price:
        settings["max_price"] = args.max_price
    if args.min_floor:
        settings["min_floor"] = args.min_floor
    if args.max_floor:
        settings["max_floor"] = args.max_floor
    if args.house_type:
        settings["house_material_type"] = args.house_type
    if args.min_year:
        settings["min_house_year"] = args.min_year
    if args.no_shares:
        settings["flat_share"] = 2
    if args.owner_only:
        settings["is_by_homeowner"] = True
    if args.metro:
        settings["metro"] = "Московский"
        settings["metro_station"] = args.metro
    settings["sort_by"] = "price_from_min_to_max"

    return settings


def parse_rooms(rooms_str):
    if rooms_str is None:
        return "all"
    parts = [r.strip() for r in rooms_str.split(",")]
    if len(parts) == 1:
        if parts[0] == "studio":
            return "studio"
        return int(parts[0])
    return tuple(int(r) if r != "studio" else "studio" for r in parts)


def filter_by_address(data, address_query):
    query = address_query.lower()
    return [
        item for item in data
        if query in item.get("street", "").lower()
        or query in item.get("house_number", "").lower()
        or query in item.get("district", "").lower()
        or query in item.get("residential_complex", "").lower()
    ]


def calculate_flip_profit(market_price_per_m2, total_meters, auction_price, renovation_cost=None):
    """Расчёт прибыльности FLIP-сделки."""
    market_price = market_price_per_m2 * total_meters

    if renovation_cost is None:
        renovation_cost = int(total_meters * 45_000)

    total_investment = auction_price + renovation_cost
    sale_price_with_renovation = market_price * 1.15
    sale_price = int(sale_price_with_renovation * 0.95)

    gross_profit = sale_price - total_investment

    deductible = auction_price + int(renovation_cost * 0.6)
    taxable = sale_price - deductible
    tax = max(0, int(taxable * 0.05))
    net_profit = gross_profit - tax

    discount_from_market = (1 - auction_price / market_price) * 100

    return {
        "market_price": market_price,
        "auction_price": auction_price,
        "discount_from_market": discount_from_market,
        "renovation_cost": renovation_cost,
        "total_investment": total_investment,
        "sale_price": sale_price,
        "gross_profit": gross_profit,
        "tax": tax,
        "net_profit": net_profit,
        "per_partner": net_profit // 2,
        "roi_deal": net_profit / total_investment * 100,
        "roi_annual": net_profit / total_investment * 100 * 6,
    }


def print_analysis(data, title=""):
    if not data:
        print("\n❌ Нет данных для анализа")
        return None

    prices = [item["price"] for item in data if item.get("price", 0) > 0]
    meters = [item["total_meters"] for item in data if item.get("total_meters", 0) > 0]
    price_per_m2 = [
        item["price"] / item["total_meters"]
        for item in data
        if item.get("price", 0) > 0 and item.get("total_meters", 0) > 0
    ]

    if not prices:
        print("\n❌ Нет объявлений с ценами")
        return None

    avg_price = sum(prices) / len(prices)
    min_price = min(prices)
    max_price = max(prices)
    median_price = sorted(prices)[len(prices) // 2]
    avg_m2 = sum(meters) / len(meters) if meters else 0
    avg_price_m2 = sum(price_per_m2) / len(price_per_m2) if price_per_m2 else 0
    median_price_m2 = sorted(price_per_m2)[len(price_per_m2) // 2] if price_per_m2 else 0

    print(f"\n{'=' * 60}")
    print(f"  📊 АНАЛИЗ РЫНКА: {title}")
    print(f"{'=' * 60}")
    print(f"  Найдено объявлений:     {len(data)}")
    print(f"  Средняя площадь:        {avg_m2:.1f} м²")
    print(f"{'─' * 60}")
    print(f"  💰 ЦЕНЫ:")
    print(f"  Средняя цена:           {format_price(avg_price)}")
    print(f"  Медианная цена:         {format_price(median_price)}")
    print(f"  Мин. цена:              {format_price(min_price)}")
    print(f"  Макс. цена:             {format_price(max_price)}")
    print(f"{'─' * 60}")
    print(f"  📐 ЦЕНА ЗА М²:")
    print(f"  Средняя за м²:          {format_price(int(avg_price_m2))}")
    print(f"  Медианная за м²:        {format_price(int(median_price_m2))}")
    print(f"{'=' * 60}")

    print(f"\n  🏠 ТОП-10 САМЫХ ДЕШЁВЫХ:")
    print(f"  {'─' * 56}")
    sorted_data = sorted(
        [d for d in data if d.get("price", 0) > 0],
        key=lambda x: x["price"]
    )
    for i, item in enumerate(sorted_data[:10], 1):
        street = item.get("street", "?")
        house = item.get("house_number", "")
        rooms = item.get("rooms_count", "?")
        m2 = item.get("total_meters", 0)
        price = item.get("price", 0)
        floor = item.get("floor", "?")
        floors = item.get("floors_count", "?")
        pm2 = int(price / m2) if m2 > 0 else 0

        print(f"  {i:2}. {format_price(price):>12} | {m2:5.1f} м² | {rooms}-комн | "
              f"эт. {floor}/{floors} | {pm2:,} ₽/м²".replace(",", " "))
        print(f"      {street} {house}")
        if item.get("url"):
            print(f"      🔗 {item['url']}")

    return {
        "count": len(data),
        "avg_price": avg_price,
        "median_price": median_price,
        "avg_price_m2": avg_price_m2,
        "median_price_m2": median_price_m2,
        "avg_meters": avg_m2,
    }


def save_results(data, filename):
    if not data:
        return
    filepath = os.path.join(os.path.dirname(__file__), filename)
    keys = data[0].keys()
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    print(f"\n  💾 Данные сохранены: {filepath}")


def main():
    args = parse_args()

    print(f"\n{'=' * 60}")
    print(f"  🏗️  FLIP-ПРОЕКТ: Анализатор ЦИАН")
    print(f"  📍 Локация: {args.location}")
    print(f"  📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"{'=' * 60}")

    p = cianparser.CianParser(location=args.location)
    rooms = parse_rooms(args.rooms)
    settings = build_settings(args)

    print(f"\n  ⏳ Парсинг ЦИАН (страницы 1–{args.pages})...")
    print(f"  Комнаты: {rooms}")
    print(f"  Настройки: {json.dumps(settings, ensure_ascii=False, indent=4)}")
    print()

    data = p.get_flats(
        deal_type="sale",
        rooms=rooms,
        with_saving_csv=args.save_csv,
        with_extra_data=args.extra_data,
        additional_settings=settings,
    )

    if args.address:
        print(f"\n  🔍 Фильтр по адресу: «{args.address}»")
        filtered = filter_by_address(data, args.address)
        stats_filtered = print_analysis(filtered, f"{args.address} (отфильтровано)")

        if stats_filtered and len(filtered) < len(data):
            print(f"\n  Также общая статистика по всей выборке:")
            stats_all = print_analysis(data, f"{args.location} (все)")
    else:
        stats_all = print_analysis(data, args.location)

    if args.auction_price:
        working_data = filtered if args.address and 'filtered' in dir() else data
        if working_data:
            price_per_m2_list = [
                item["price"] / item["total_meters"]
                for item in working_data
                if item.get("price", 0) > 0 and item.get("total_meters", 0) > 0
            ]
            if price_per_m2_list:
                avg_m2_price = sum(price_per_m2_list) / len(price_per_m2_list)
                avg_meters = sum(
                    item["total_meters"]
                    for item in working_data
                    if item.get("total_meters", 0) > 0
                ) / len([i for i in working_data if i.get("total_meters", 0) > 0])

                flip = calculate_flip_profit(
                    avg_m2_price, avg_meters,
                    args.auction_price,
                    args.renovation_cost
                )

                print(f"\n{'=' * 60}")
                print(f"  💼 РАСЧЁТ FLIP-СДЕЛКИ")
                print(f"{'=' * 60}")
                print(f"  Рыночная цена (без рем.): {format_price(int(flip['market_price']))}")
                print(f"  Цена на торгах:           {format_price(flip['auction_price'])}")
                print(f"  Дисконт от рынка:         {flip['discount_from_market']:.1f}%")
                print(f"{'─' * 60}")
                print(f"  Ремонт + материалы:       {format_price(flip['renovation_cost'])}")
                print(f"  Полная себестоимость:      {format_price(flip['total_investment'])}")
                print(f"  Цена продажи (−5% рынка): {format_price(flip['sale_price'])}")
                print(f"{'─' * 60}")
                print(f"  Валовая прибыль:          {format_price(flip['gross_profit'])}")
                print(f"  Налог УСН 5%:             {format_price(flip['tax'])}")
                print(f"  ✅ Чистая прибыль:         {format_price(flip['net_profit'])}")
                print(f"  👤 На каждого партнёра:    {format_price(flip['per_partner'])}")
                print(f"  📈 ROI сделки:             {flip['roi_deal']:.1f}%")
                print(f"  📈 ROI годовой (×6):       {flip['roi_annual']:.1f}%")
                verdict = "✅ ВЫГОДНО" if flip['net_profit'] > 500_000 else "⚠️ МАРЖА МАЛА" if flip['net_profit'] > 0 else "❌ УБЫТОК"
                print(f"  {'─' * 56}")
                print(f"  Вердикт:                  {verdict}")
                print(f"{'=' * 60}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    rooms_str = str(rooms).replace(" ", "")
    filename = f"cian_analysis_{args.location}_{rooms_str}_{timestamp}.csv"
    save_results(data, filename)


if __name__ == "__main__":
    main()
