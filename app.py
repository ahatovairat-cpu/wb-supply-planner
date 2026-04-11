"""
StockPilot — локальный веб-сервер
Запуск: python app.py
Откройте браузер: http://localhost:5000
"""
from flask import Flask, render_template, jsonify, request, session
import requests as req
import pandas as pd
from datetime import datetime, timedelta
import time, json, os, math

app = Flask(__name__)
app.secret_key = "wb_supply_planner_v3_secret_key_2026"

# ── SQLite для сохранения параметров артикулов между перезапусками ──
import sqlite3

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sku_params.db")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS sku_params (
        sku TEXT PRIMARY KEY,
        cost REAL DEFAULT 0,
        archived INTEGER DEFAULT 0,
        source TEXT DEFAULT 'Россия',
        lead_days INTEGER DEFAULT 2,
        box_qty INTEGER DEFAULT 1,
        ff_stock INTEGER DEFAULT 0
    )""")
    conn.commit()
    conn.close()

def load_saved_params():
    """Загружает сохранённые параметры из SQLite."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM sku_params").fetchall()
    conn.close()
    return {r["sku"]: dict(r) for r in rows}

def save_params_to_db(params_dict):
    """Сохраняет параметры артикулов в SQLite."""
    conn = sqlite3.connect(DB_PATH)
    for sku, p in params_dict.items():
        conn.execute("""INSERT OR REPLACE INTO sku_params 
            (sku, cost, archived, source, lead_days, box_qty, ff_stock) 
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (sku, float(p.get("cost", 0)), int(p.get("archived", 0)),
             str(p.get("source", "Россия")), int(p.get("lead_days", 2)),
             int(p.get("box_qty", 1)), int(p.get("ff_stock", 0))))
    conn.commit()
    conn.close()

init_db()

BASE = "https://statistics-api.wildberries.ru/api/v1"
BASE_CONTENT = "https://content-api.wildberries.ru"

# ── Per-session store ──
# Каждый пользователь получает свой изолированный store по session_id.
# Данные хранятся в _stores[session_id] и не пересекаются.
_stores = {}

def get_store():
    """Возвращает store текущей сессии. Создаёт если нет."""
    import uuid
    if "sid" not in session:
        session["sid"] = uuid.uuid4().hex
    sid = session["sid"]
    if sid not in _stores:
        _stores[sid] = {}
    return _stores[sid]

def hdrs(token):
    return {"Authorization": token, "Content-Type": "application/json"}

def safe_get(url, token, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = req.get(url, headers=hdrs(token), params=params, timeout=60)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 60))
                time.sleep(wait); continue
            if r.status_code in (401, 403):
                return {"error": "Неверный токен или нет доступа"}
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries - 1:
                return {"error": str(e)}
            time.sleep(3)
    return {"error": "Превышено число попыток"}

def find_col(df, candidates):
    return next((c for c in candidates if c in df.columns), None)


def fetch_report_logistics(token, date_from, date_to):
    """
    Загружает отчёт реализации (v5) и считает среднюю логистику на единицу по артикулу.
    Возвращает dict {supplierArticle: avg_logistics_per_unit}
    """
    try:
        url = f"{BASE.replace('/v1', '/v5')}/supplier/reportDetailByPeriod"
        all_rows = []
        rrdid = 0
        max_pages = 3  # 7 дней = мало данных, 3 страниц достаточно
        
        for page in range(max_pages):
            params = {
                "dateFrom": date_from[:10],
                "dateTo": date_to[:10],
                "rrdid": rrdid,
                "limit": 50000
            }
            # Увеличенный таймаут для тяжёлого отчёта
            try:
                import sys
                print(f"[REPORT] запрос: dateFrom={params['dateFrom']} dateTo={params['dateTo']} rrdid={rrdid}", flush=True)
                r = req.get(url, headers=hdrs(token), params=params, timeout=90)
                print(f"[REPORT] ответ: HTTP {r.status_code}, len={len(r.text)}", flush=True)
                if r.status_code == 204:
                    print(f"[REPORT] HTTP 204 — конец данных", flush=True)
                    break
                if r.status_code != 200:
                    print(f"[REPORT] ошибка HTTP {r.status_code}: {r.text[:300]}", flush=True)
                    return {}
                data = r.json()
                if isinstance(data, list):
                    print(f"[REPORT] получено {len(data)} строк", flush=True)
                elif isinstance(data, dict):
                    print(f"[REPORT] dict: {list(data.keys())[:10]}", flush=True)
                else:
                    print(f"[REPORT] неизвестный тип: {type(data)}", flush=True)
            except Exception as e:
                print(f"[REPORT] exception: {e}", flush=True)
                return {}
            if isinstance(data, dict) and "error" in data:
                print(f"[REPORT] ошибка: {data['error']}")
                return {}
            if not data or not isinstance(data, list) or len(data) == 0:
                break
            all_rows.extend(data)
            # Следующая страница
            last_rrdid = data[-1].get("rrd_id", 0)
            if last_rrdid == rrdid:
                break
            rrdid = last_rrdid
        
        if not all_rows:
            print("[REPORT]: 0 строк")
            return {}
        
        print(f"[REPORT]: {len(all_rows)} строк", flush=True)
        
        # Лог первой строки для диагностики полей
        if all_rows:
            sample = all_rows[0]
            print(f"[REPORT] поля: {list(sample.keys())}", flush=True)
            # Ищем поля с delivery/logistics
            delivery_fields = [k for k in sample.keys() if 'delivery' in k.lower() or 'logistic' in k.lower() or 'log' in k.lower()]
            print(f"[REPORT] поля логистики: {delivery_fields}", flush=True)
            for f in delivery_fields:
                print(f"[REPORT]   {f} = {sample[f]}", flush=True)
            # Покажем supplierArticle и doc_type
            print(f"[REPORT] sa_name={sample.get('sa_name','?')}, supplierArticle={sample.get('supplierArticle','?')}, doc_type_name={sample.get('doc_type_name','?')}", flush=True)
            # Показываем первую строку с Продажа
            for r in all_rows[:50]:
                if 'Продажа' in str(r.get('doc_type_name','')):
                    print(f"[REPORT] ПРОДАЖА: sa_name={r.get('sa_name','?')}, delivery_rub={r.get('delivery_rub',0)}, quantity={r.get('quantity',0)}", flush=True)
                    break
        
        # Группируем по supplierArticle, считаем среднюю логистику
        # Берём строки "Продажа" и "Возврат" — сумму логистики делим на кол-во продаж
        from collections import defaultdict
        sku_logistics = defaultdict(lambda: {"total_delivery": 0, "sales_count": 0})
        
        doc_types_found = set()
        for row in all_rows:
            sa = str(row.get("sa_name", "") or row.get("supplierArticle", ""))
            delivery = float(row.get("delivery_rub", 0) or 0)
            doc_type = str(row.get("doc_type_name", ""))
            doc_types_found.add(doc_type)
            
            if not sa:
                continue
            
            # Суммируем логистику со ВСЕХ строк где delivery_rub > 0
            if delivery > 0:
                sku_logistics[sa]["total_delivery"] += delivery
            
            # Считаем только продажи для деления
            if "Продажа" in doc_type:
                sku_logistics[sa]["sales_count"] += 1
        
        print(f"[REPORT] типы документов: {doc_types_found}", flush=True)
        
        result = {}
        for sa, data in sku_logistics.items():
            if data["sales_count"] > 0:
                result[sa] = round(data["total_delivery"] / data["sales_count"], 1)
        
        print(f"[REPORT] логистика: {len(result)} артикулов", flush=True)
        if result:
            sample_items = list(result.items())[:3]
            print(f"[REPORT] примеры: {sample_items}", flush=True)
        else:
            # Диагностика: сколько артикулов с delivery но без продаж
            has_delivery = sum(1 for d in sku_logistics.values() if d["total_delivery"] > 0)
            has_sales = sum(1 for d in sku_logistics.values() if d["sales_count"] > 0)
            print(f"[REPORT] диагностика: {len(sku_logistics)} артикулов в dict, {has_delivery} с delivery, {has_sales} с продажами", flush=True)
            if sku_logistics:
                sample_sa = list(sku_logistics.items())[:2]
                print(f"[REPORT] примеры: {sample_sa}", flush=True)
        return result
        
    except Exception as e:
        print(f"fetch_report_logistics error: {e}")
        return {}

# ────────────────────────────────────────────
# ROUTES
# ────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

# ── 1. Проверка токена ──
@app.route("/api/check_token", methods=["POST"])
def check_token():
    token = request.json.get("token", "").strip()
    if not token:
        return jsonify({"ok": False, "error": "Токен не указан"})
    date_from = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
    result = safe_get(f"{BASE}/supplier/stocks", token, {"dateFrom": date_from})
    if isinstance(result, dict) and "error" in result:
        return jsonify({"ok": False, "error": result["error"]})
    get_store()["token"] = token
    # Сбрасываем archived при каждом новом подключении
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("UPDATE sku_params SET archived = 0")
        conn.commit()
        conn.close()
    except:
        pass
    return jsonify({"ok": True})

# ── 2. Загрузка всех данных ──
@app.route("/api/load_data", methods=["POST"])
def load_data():
    token = get_store().get("token")
    if not token:
        return jsonify({"ok": False, "error": "Сначала введите токен"})

    days = int(request.json.get("days", 30))
    # Грузим days + 14 дней для расчёта тренда (предыдущий период)
    trend_extra = 14
    date_from_full = (datetime.now() - timedelta(days=days + trend_extra)).strftime("%Y-%m-%dT00:00:00")
    date_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00")
    date_from_1d  = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
    # Для остатков дата максимально далеко — API возвращает только записи изменённые после dateFrom
    date_from_stocks = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%dT00:00:00")

    # Продажи — грузим с запасом для тренда
    sales_data = safe_get(f"{BASE}/supplier/sales", token, {"dateFrom": date_from_full})
    if isinstance(sales_data, dict) and "error" in sales_data:
        return jsonify({"ok": False, "error": "Продажи: " + sales_data["error"]})

    # Остатки
    stocks_data = safe_get(f"{BASE}/supplier/stocks", token, {"dateFrom": date_from_stocks})
    if isinstance(stocks_data, dict) and "error" in stocks_data:
        return jsonify({"ok": False, "error": "Остатки: " + stocks_data["error"]})

    # Заказы — грузим с запасом для тренда
    orders_data = safe_get(f"{BASE}/supplier/orders", token, {"dateFrom": date_from_full})
    if isinstance(orders_data, dict) and "error" in orders_data:
        return jsonify({"ok": False, "error": "Заказы: " + orders_data["error"]})

    # Возвраты берём из того же эндпоинта продаж — isReturn=True
    returns_data = []  # считается из orders_data ниже

    # Габариты — через Контент API (если токен имеет права)
    dims_data = fetch_dimensions(token)

    # Отчёт реализации загружается отдельной кнопкой в плане отгрузок
    report_logistics = {}

    # Обработка — передаём основной период и дату начала основного периода
    result = process_data(sales_data or [], stocks_data or [], orders_data or [], dims_data, days, date_from)
    result["trend_extra"] = trend_extra  # сколько дней в предыдущем периоде для тренда
    
    # Подставляем реальную логистику из отчёта реализации
    if report_logistics:
        for sku, info in result["skus"].items():
            sa = sku  # supplierArticle
            if sa in report_logistics:
                info["avg_logistics"] = report_logistics[sa]
    
    get_store()["data"] = result
    get_store()["days"] = days

    return jsonify({
        "ok": True,
        "summary": {
            "skus": len(result["skus"]),
            # Склады где реально есть остаток > 0
            "warehouses": len(set(
                wh for sku_data in result["skus"].values()
                for wh, wh_data in sku_data["warehouses"].items()
                if wh_data.get("stock", 0) > 0
            )),
            "total_sales": int(result["total_sales"]),
            "total_stock": int(result["total_stock"]),
        }
    })

def fetch_dimensions(token):
    """Получаем габариты товаров через Контент API v2 с пагинацией.
    Если Контент API недоступен — габариты будут пустыми (не критично).
    """
    dims = {}
    try:
        url    = f"{BASE_CONTENT}/content/v2/get/cards/list"
        cursor = {}
        total  = 0

        while True:
            body = {"settings": {"cursor": dict(limit=100, **cursor), "filter": {"withPhoto": -1}}}
            r = req.post(url, headers=hdrs(token), json=body, timeout=30)
            if r.status_code != 200:
                print(f"Контент API ошибка {r.status_code} — габариты недоступны")
                break

            data  = r.json()
            cards = data.get("cards", [])
            total += len(cards)

            if total == 0:
                print("Контент API вернул 0 карточек — возможно токен без прав на контент")
                break

            for card in cards:
                sku   = card.get("vendorCode", "")
                nm_id = str(card.get("nmID") or card.get("nmId") or "")

                dim_obj = card.get("dimensions") or {}
                length  = float(dim_obj.get("length") or 0)
                width   = float(dim_obj.get("width")  or 0)
                height  = float(dim_obj.get("height") or 0)
                volume  = round(length * width * height / 1000, 2) if length and width and height else 0.0

                title = card.get("title", "") or card.get("name", "") or ""
                subject = card.get("subjectName", "") or card.get("subject", "") or ""
                entry = {"length": length, "width": width, "height": height, "volume": volume, "title": title, "subject": subject}
                if sku:
                    dims[sku] = entry
                if nm_id:
                    dims[nm_id] = entry

            cur = data.get("cursor") or {}
            cursor_total = cur.get("total", 0)
            if len(cards) < 100:
                break  # последняя страница — карточек меньше лимита
            cursor = {"updatedAt": cur.get("updatedAt"), "nmID": cur.get("nmID")}
            if not cursor.get("updatedAt") and not cursor.get("nmID"):
                break  # нет данных для следующей страницы

        print(f"Габариты: {total} карточек, {len(dims)} записей")
    except Exception as e:
        print(f"Габариты ошибка: {e}")
    return dims


def fetch_dimensions_by_nmids(token, nmids):
    """Запрашиваем габариты по конкретным nmID через Контент API."""
    dims = {}
    try:
        if not nmids:
            return dims
        # Разбиваем на батчи по 100
        for i in range(0, len(nmids), 100):
            batch = [int(n) for n in nmids[i:i+100] if str(n).isdigit()]
            if not batch:
                continue
            url  = f"{BASE_CONTENT}/content/v2/get/cards/list"
            body = {"settings": {"cursor": {"limit": 100}, "filter": {"withPhoto": -1, "nmID": batch[0]}}}
            r = req.post(url, headers=hdrs(token), json=body, timeout=30)
            if r.status_code == 200:
                for card in r.json().get("cards", []):
                    nm_id = str(card.get("nmID") or "")
                    sku   = card.get("vendorCode", "")
                    dim_obj = card.get("dimensions") or {}
                    length  = float(dim_obj.get("length") or 0)
                    width   = float(dim_obj.get("width")  or 0)
                    height  = float(dim_obj.get("height") or 0)
                    volume  = round(length * width * height / 1000, 2) if length and width and height else 0.0
                    title = card.get("title", "") or card.get("name", "") or ""
                    subject = card.get("subjectName", "") or card.get("subject", "") or ""
                    entry = {"length": length, "width": width, "height": height, "volume": volume, "title": title, "subject": subject}
                    if nm_id: dims[nm_id] = entry
                    if sku:   dims[sku]   = entry
    except Exception as e:
        print(f"fetch_dimensions_by_nmids: {e}")
    return dims

# Только реальные склады WB где можно хранить товар
# Исключаем СЦ (сортировочные центры), зарубежные и прочие нерелевантные
def is_wb_warehouse(name):
    if not name:
        return False
    n = name.strip()
    # Исключаем сортировочные центры
    if n.startswith("СЦ "):
        return False
    # Исключаем зарубежные
    foreign = ["Астана", "Брест", "Гродно", "Ереван", "Бишкек", "Алматы", "Шымкент", "Минск"]
    if any(n.startswith(f) for f in foreign):
        return False
    # Исключаем явно лишние
    skip = ["Остальные склады", "Артём", "Атакент"]
    if n in skip:
        return False
    return True


def calc_trend(daily_sales, period_days):
    """
    Тренд по MA7 + понедельная проверка устойчивости.
    
    daily_sales: dict {date_str: count} — продажи по дням
    period_days: int — длина периода
    
    Возвращает dict:
      coef: float — коэффициент тренда (0.7 - 1.3)
      direction: str — 'up', 'down', 'stable'
      reliable: bool — устойчивый ли тренд
      weeks: list — продажи по неделям
    """
    if not daily_sales or period_days < 28:
        return {"coef": 1.0, "direction": "stable", "reliable": False, "weeks": []}

    # Собираем продажи по дням за период (заполняем нулями пропущенные дни)
    from datetime import datetime, timedelta
    today = datetime.now().date()
    day_counts = []
    for i in range(period_days, 0, -1):
        d = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        day_counts.append(daily_sales.get(d, 0))

    # ── Шаг 1: Понедельные итоги ──
    num_weeks = len(day_counts) // 7
    weeks = []
    for w in range(num_weeks):
        week_sales = sum(day_counts[w*7 : (w+1)*7])
        weeks.append(week_sales)

    if len(weeks) < 4:
        return {"coef": 1.0, "direction": "stable", "reliable": False, "weeks": weeks}

    # ── Шаг 2: MA7 направление ──
    # Сравниваем среднее последних 2 недель vs первых 2 недель
    first_half = sum(weeks[:2]) / 2.0
    second_half = sum(weeks[-2:]) / 2.0

    if first_half <= 0:
        return {"coef": 1.0, "direction": "stable", "reliable": False, "weeks": weeks}

    raw_trend = second_half / first_half

    # ── Шаг 3: Проверка устойчивости ──
    # Считаем сколько недель растут/падают подряд
    up_count = 0
    down_count = 0
    for i in range(1, len(weeks)):
        if weeks[i] > weeks[i-1]:
            up_count += 1
        elif weeks[i] < weeks[i-1]:
            down_count += 1

    total_transitions = len(weeks) - 1
    # Устойчивый рост: >60% недель растут
    # Устойчивое падение: >60% недель падают
    reliable = False
    if up_count / total_transitions >= 0.6 and raw_trend > 1.05:
        reliable = True
        direction = "up"
    elif down_count / total_transitions >= 0.6 and raw_trend < 0.95:
        reliable = True
        direction = "down"
    else:
        direction = "stable"

    # ── Шаг 4: Коэффициент с ограничением ±50% ──
    if reliable:
        coef = max(0.5, min(1.5, raw_trend))
    else:
        coef = 1.0  # Нет устойчивости → не применяем тренд

    return {
        "coef": round(coef, 2),
        "direction": direction,
        "reliable": reliable,
        "weeks": weeks,
        "raw": round(raw_trend, 2),
    }


# ── Сезонность из MP Stats ──
TRENDS_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wb_trends.db")

def get_season_coefs(subject_name):
    """
    Возвращает коэффициенты сезонности по предмету WB.
    Коэф = продажи_месяца / среднее_за_год.
    Для 2025 берём данные 2025, для остальных месяцев — 2024.
    """
    if not os.path.exists(TRENDS_DB):
        return None

    try:
        conn = sqlite3.connect(TRENDS_DB)
        cursor = conn.cursor()

        # Ищем предмет по имени (точное совпадение)
        cursor.execute("SELECT id FROM subjects WHERE name = ?", (subject_name,))
        row = cursor.fetchone()
        if not row:
            # Пробуем частичное совпадение
            cursor.execute("SELECT id FROM subjects WHERE name LIKE ?", (f'%{subject_name}%',))
            row = cursor.fetchone()
        if not row:
            conn.close()
            return None

        subject_id = row[0]

        # Берём все месячные данные
        cursor.execute("SELECT month, total_sales FROM subject_monthly WHERE subject_id=? ORDER BY month", (subject_id,))
        data = cursor.fetchall()
        conn.close()

        if not data:
            return None

        # Разделяем по годам
        by_year = {}
        for month_str, sales in data:
            year = month_str[:4]
            if year not in by_year:
                by_year[year] = {}
            by_year[year][month_str[5:7]] = sales

        # Считаем средние по годам
        avg_by_year = {}
        for year, months in by_year.items():
            avg_by_year[year] = sum(months.values()) / len(months) if months else 1

        # Коэф для каждого месяца: берём из последнего доступного года
        coefs = {}
        for mm in ['01','02','03','04','05','06','07','08','09','10','11','12']:
            # Сначала пробуем 2025, потом 2024
            for year in sorted(by_year.keys(), reverse=True):
                if mm in by_year[year] and avg_by_year[year] > 0:
                    coefs[mm] = round(by_year[year][mm] / avg_by_year[year], 3)
                    break
            if mm not in coefs:
                coefs[mm] = 1.0

        return coefs

    except Exception as e:
        print(f"Сезонность ошибка: {e}")
        return None


def get_season_multiplier(subject_name, current_month, next_month):
    """
    Возвращает сезонный множитель = коэф_следующего / коэф_текущего.
    current_month, next_month — строки '01'..'12'
    """
    coefs = get_season_coefs(subject_name)
    if not coefs:
        return 1.0, None

    cur = coefs.get(current_month, 1.0)
    nxt = coefs.get(next_month, 1.0)

    if cur <= 0:
        return 1.0, coefs

    multiplier = round(nxt / cur, 2)
    # Ограничиваем ±50%
    multiplier = max(0.5, min(1.5, multiplier))
    return multiplier, coefs


def process_data(sales_raw, stocks_raw, orders_raw, dims, days, main_date_from=None):
    result = {"skus": {}, "warehouses": set(), "total_sales": 0, "total_stock": 0}

    # ── Продажи — считаем два периода: текущий (days) и предыдущий (14 дней до него) ──
    if sales_raw:
        df = pd.DataFrame(sales_raw)
        c_sku  = find_col(df, ["supplierArticle", "sa_name", "article"])
        c_name = find_col(df, ["supplierArticle", "sa_name", "article"])
        c_date = find_col(df, ["date", "saleDate", "lastChangeDate"])
        c_nmid = find_col(df, ["nmId", "nmID", "nm_id", "nmid"])
        if "isRealization" in df.columns:
            df = df[df["isRealization"] == True]

        # Делим на два периода по дате
        if c_date and main_date_from:
            df[c_date] = pd.to_datetime(df[c_date], errors="coerce", utc=True).dt.tz_localize(None)
            cutoff = pd.to_datetime(main_date_from)
            df_curr = df[df[c_date] >= cutoff]   # основной период
            df_prev = df[df[c_date] <  cutoff]   # предыдущий (для тренда)
        else:
            df_curr = df
            df_prev = pd.DataFrame()

        if c_sku and len(df_curr):
            for sku_val, grp in df_curr.groupby(c_sku):
                sku = str(sku_val)
                sales_cnt = len(grp)
                # Дата первой продажи в периоде — для расчёта реального темпа
                first_sale_date = None
                daily_sales = {}
                if c_date:
                    dates = grp[c_date].dropna()
                    if len(dates):
                        first_sale_date = dates.min()
                        # Продажи по дням для тренда
                        for d in dates:
                            day_key = d.strftime('%Y-%m-%d')
                            daily_sales[day_key] = daily_sales.get(day_key, 0) + 1
                # Продажи предыдущего периода для тренда
                prev_cnt = len(df_prev[df_prev[c_sku] == sku_val]) if len(df_prev) else 0
                if sku not in result["skus"]:
                    name = grp[c_name].iloc[0] if c_name else sku
                    nmid = str(int(grp[c_nmid].iloc[0])) if c_nmid and not grp[c_nmid].isna().all() else ""
                    sku_dims = dims.get(sku) or dims.get(nmid) or {}
                card_title = (dims.get(sku) or dims.get(nmid) or {}).get("title", "")
                display_name = card_title if card_title else sku
                result["skus"][sku] = {"name": display_name, "nmid": nmid, "sales": 0,
                                           "sales_prev": 0, "stock_total": 0,
                                           "warehouses": {}, "cost": 0, "dims": sku_dims}
                result["skus"][sku]["sales"]      = sales_cnt
                result["skus"][sku]["sales_prev"] = prev_cnt
                # Считаем реальные дни продаж (от первой продажи до конца периода)
                if first_sale_date is not None:
                    now = pd.Timestamp.now()
                    active_days = max(1, (now - first_sale_date).days)
                    result["skus"][sku]["active_days"] = min(active_days, days)
                else:
                    result["skus"][sku]["active_days"] = days

                # ── Тренд: MA7 + понедельная проверка устойчивости ──
                result["skus"][sku]["trend"] = calc_trend(daily_sales, days)

                result["total_sales"] += sales_cnt

    # ── Остатки ──
    # stock_total = quantityFull (все склады WB включая СЦ) + inWayFromClient (возвраты едут обратно)
    if stocks_raw:
        df = pd.DataFrame(stocks_raw)
        c_sku      = find_col(df, ["supplierArticle","sa_name","article"])
        c_wh       = find_col(df, ["warehouseName","warehouse","wh_name"])
        c_qty      = find_col(df, ["quantityFull","quantity","cnt","stock"])  # полные остатки
        c_way_to   = find_col(df, ["inWayToClient"])
        c_way_from = find_col(df, ["inWayFromClient"])
        c_price    = find_col(df, ["Price"])
        c_discount = find_col(df, ["Discount"])
        if c_sku and c_qty:
            df["_qty"]      = pd.to_numeric(df[c_qty],      errors="coerce").fillna(0)
            df["_way_to"]   = pd.to_numeric(df[c_way_to],   errors="coerce").fillna(0) if c_way_to else 0
            df["_way_from"] = pd.to_numeric(df[c_way_from], errors="coerce").fillna(0) if c_way_from else 0

            # Группируем по артикулу — суммируем ВСЕ строки (включая СЦ)
            # Логируем уникальные склады для диагностики
            all_wh_names = set(df[c_wh].astype(str).unique()) if c_wh else set()
            our_wh_names = set(WAREHOUSE_REGION.keys())
            found_our = all_wh_names & our_wh_names
            not_found = our_wh_names - all_wh_names
            extra_whs = sorted([w for w in all_wh_names if is_wb_warehouse(w) and w not in our_wh_names])
            print(f"Склады в остатках API: {len(all_wh_names)} всего, наших найдено: {sorted(found_our)}")
            if not_found:
                print(f"  Наши склады НЕ найдены в API: {sorted(not_found)}")
            if extra_whs:
                print(f"  Склады в API, но не в WAREHOUSE_REGION: {extra_whs[:15]}")

            for sku_val, grp in df.groupby(c_sku):
                sku      = str(sku_val)
                total_q  = int(grp["_qty"].sum())
                total_wt = int(grp["_way_to"].sum()) if c_way_to else 0
                total_wf = int(grp["_way_from"].sum()) if c_way_from else 0

                if sku not in result["skus"]:
                    result["skus"][sku] = {"name": sku, "sales": 0, "stock_total": 0,
                                           "warehouses": {}, "cost": 0, "dims": {}}

                # stock_total = quantityFull - inWayToClient + inWayFromClient
                # quantityFull включает inWayToClient, но товар в пути к клиенту уже не на складе
                # inWayFromClient — едет на склад WB, прибавляем
                result["skus"][sku]["stock_total"] = total_q - total_wt + total_wf
                result["total_stock"] += total_q - total_wt + total_wf
                # Товар, который едет на склад WB (возвраты от клиентов)
                result["skus"][sku]["in_way_to_wh"] = total_wf
                
                # Цена продажи из API остатков (Price × (1 - Discount/100))
                if c_price:
                    first_row = grp.iloc[0]
                    raw_price = float(first_row.get(c_price, 0) or 0)
                    discount = float(first_row.get(c_discount, 0) or 0) if c_discount else 0
                    retail_price = round(raw_price * (1 - discount / 100), 0)
                    if retail_price > 0:
                        result["skus"][sku]["retail_price"] = retail_price

                # warehouses — все склады с остатком (для отображения)
                # result["warehouses"] — только наши (для локализации и рекомендаций)
                if c_wh:
                    for wh_name, wh_sub in grp.groupby(c_wh):
                        wh = str(wh_name)
                        qty = int(wh_sub["_qty"].sum())
                        wt = int(wh_sub["_way_to"].sum()) if c_way_to else 0
                        wf = int(wh_sub["_way_from"].sum()) if c_way_from else 0
                        # Реально на складе + едет на этот склад (возвраты)
                        stock_on_wh = qty - wt + wf
                        if stock_on_wh <= 0:
                            continue
                        if wh not in result["skus"][sku]["warehouses"]:
                            result["skus"][sku]["warehouses"][wh] = {"stock": 0, "orders": 0, "local_pct": 0}
                        result["skus"][sku]["warehouses"][wh]["stock"] += stock_on_wh
                        if is_wb_warehouse(wh):
                            result["warehouses"].add(wh)


    # ── Заказы — локализация + география покупателей ──
    # Локальный заказ = покупатель и склад в одном ФО (по всем складам включая чужие)
    if orders_raw:
        df = pd.DataFrame(orders_raw)
        c_sku    = find_col(df, ["supplierArticle","sa_name","article"])
        c_wh     = find_col(df, ["warehouseName","warehouse","storeId"])
        c_okrug  = find_col(df, ["oblastOkrugName","oblast_okrug","okrugName"])
        c_cancel = find_col(df, ["isCancel","cancel","cancelled"])
        if c_sku and c_wh:
            if c_cancel:
                df = df[df[c_cancel] == False]

            # Строим маппинг склад → кластер для ВСЕХ складов (не только наших)
            # Для чужих складов используем частичное совпадение названий
            _wh_cluster_cache = {}
            def get_wh_cluster(wh_name):
                if wh_name in _wh_cluster_cache:
                    return _wh_cluster_cache[wh_name]
                # Сначала точное совпадение
                if wh_name in WAREHOUSE_REGION:
                    _wh_cluster_cache[wh_name] = WAREHOUSE_REGION[wh_name]
                    return _wh_cluster_cache[wh_name]
                # Ищем по ключевым словам
                wl = wh_name.lower()
                cluster = None
                if any(x in wl for x in ['москв', 'коледин', 'электростал', 'белая дач', 'пушкин', 'ногинск', 'чашников', 'радумля', 'белые стол', 'истра', 'вёшки']):
                    cluster = 'Центральный'
                elif any(x in wl for x in ['санкт', 'петербург', 'шушар', 'ленинград', 'калинингр', 'череповец', 'иваново']):
                    cluster = 'Северо-Западный'
                elif any(x in wl for x in ['краснодар', 'невинномысск', 'волгоград', 'ростов', 'махачкал', 'крым', 'крыловск']):
                    cluster = 'Южный'
                elif any(x in wl for x in ['казань', 'самар', 'новосемейк', 'пенза', 'нижний', 'уфа', 'сарапул', 'оренбург']):
                    cluster = 'Приволжский'
                elif any(x in wl for x in ['екатеринб', 'перспектив', 'сургут', 'тюмен', 'челябинск', 'ханты']):
                    cluster = 'Уральский'
                elif any(x in wl for x in ['новосибирск', 'докучаев', 'красноярск', 'иркутск', 'омск', 'барнаул', 'кемерово', 'томск', 'хабаровск', 'владивосток', 'улан-удэ', 'дальнегорск']):
                    cluster = 'Уральский'  # нет нашего склада → считаем как Уральский (Екатеринбург)
                _wh_cluster_cache[wh_name] = cluster
                return cluster

            # ── Векторизованный расчёт кластеров (вместо iterrows) ──
            # Предрассчитываем кластер для каждого склада и покупателя
            df["_wh_cluster"] = df[c_wh].astype(str).map(get_wh_cluster)
            if c_okrug:
                df["_buyer_cluster"] = df[c_okrug].astype(str).map(
                    lambda x: BUYER_OKRUG_TO_CLUSTER.get(x, '')
                )
                # Локальный заказ: кластеры совпадают и оба не пустые
                df["_is_local"] = (
                    (df["_wh_cluster"].notna()) &
                    (df["_wh_cluster"] != '') &
                    (df["_buyer_cluster"] != '') &
                    (df["_wh_cluster"] == df["_buyer_cluster"])
                )
            else:
                df["_buyer_cluster"] = ''
                df["_is_local"] = False

            for sku_val, grp in df.groupby(c_sku):
                sku = str(sku_val)
                total_orders = len(grp)
                if sku not in result["skus"]:
                    result["skus"][sku] = {"name": sku, "sales": 0, "stock_total": 0,
                                           "warehouses": {}, "cost": 0, "dims": dims.get(sku, {})}

                # Сохраняем количество заказов
                result["skus"][sku]["orders_count"] = total_orders
                
                # Active days по заказам (от первого заказа)
                c_date_orders = find_col(grp, ["date", "orderDate", "lastChangeDate"])
                if c_date_orders:
                    order_dates = pd.to_datetime(grp[c_date_orders], errors="coerce", utc=True).dt.tz_localize(None).dropna()
                    if len(order_dates) > 0:
                        # Фильтруем только основной период
                        if main_date_from:
                            cutoff_orders = pd.to_datetime(main_date_from)
                            order_dates_main = order_dates[order_dates >= cutoff_orders]
                            orders_in_period = len(grp[pd.to_datetime(grp[c_date_orders], errors="coerce", utc=True).dt.tz_localize(None) >= cutoff_orders]) if main_date_from else total_orders
                        else:
                            order_dates_main = order_dates
                            orders_in_period = total_orders
                        
                        result["skus"][sku]["orders_count"] = orders_in_period
                        
                        # Заказы по дням для тренда
                        if len(order_dates_main) > 0:
                            daily_orders = {}
                            for d in order_dates_main:
                                day_key = d.strftime('%Y-%m-%d')
                                daily_orders[day_key] = daily_orders.get(day_key, 0) + 1
                            # Пересчитываем тренд по заказам
                            result["skus"][sku]["trend"] = calc_trend(daily_orders, days)
                        
                        if len(order_dates_main) > 0:
                            first_order = order_dates_main.min()
                            active_days_o = max(1, (pd.Timestamp.now() - first_order).days)
                            result["skus"][sku]["active_days_orders"] = min(active_days_o, days)
                        else:
                            result["skus"][sku]["active_days_orders"] = result["skus"][sku].get("active_days", days)
                    else:
                        result["skus"][sku]["active_days_orders"] = result["skus"][sku].get("active_days", days)
                else:
                    result["skus"][sku]["active_days_orders"] = result["skus"][sku].get("active_days", days)

                # Считаем локализацию — уже предрассчитано в _is_local
                total_local = int(grp["_is_local"].sum())

                # Сохраняем поартикульную локализацию
                art_localization = round(total_local / total_orders * 100, 1) if total_orders > 0 else 0
                result["skus"][sku]["localization_pct"] = art_localization
                result["skus"][sku]["total_orders_loc"] = total_orders
                result["skus"][sku]["local_orders_cnt"]  = total_local

                # Теперь проходим по нашим складам для warehouse-level данных
                c_date_o = find_col(grp, ["date", "orderDate", "lastChangeDate"])
                for wh_val, wh_grp in grp.groupby(c_wh):
                    wh = str(wh_val)
                    if not is_wb_warehouse(wh):
                        continue
                    result["warehouses"].add(wh)
                    orders_cnt = len(wh_grp)
                    wh_cluster = WAREHOUSE_REGION.get(wh)

                    # local_pct для склада = локальные заказы с этого склада / заказы с этого склада
                    if wh_cluster and c_okrug:
                        local_cnt = int(wh_grp["_is_local"].sum())
                        local_pct = round(local_cnt / orders_cnt * 100, 2) if orders_cnt else 0
                    else:
                        local_cnt = 0
                        local_pct = 0

                    # SPD склада = заказы / active_days (от первого заказа)
                    wh_spd = 0
                    if c_date_o and orders_cnt > 0:
                        wh_dates = pd.to_datetime(wh_grp[c_date_o], errors="coerce", utc=True).dt.tz_localize(None).dropna()
                        if len(wh_dates) > 0:
                            first_order = wh_dates.min()
                            wh_active_days = max(1, (pd.Timestamp.now() - first_order).days)
                            wh_spd = round(orders_cnt / wh_active_days, 3)

                    if wh not in result["skus"][sku]["warehouses"]:
                        result["skus"][sku]["warehouses"][wh] = {"stock": 0, "orders": 0, "local_pct": 0, "local_cnt": 0, "spd": 0}
                    result["skus"][sku]["warehouses"][wh]["orders"]    = orders_cnt
                    result["skus"][sku]["warehouses"][wh]["local_pct"] = local_pct
                    result["skus"][sku]["warehouses"][wh]["local_cnt"] = local_cnt
                    result["skus"][sku]["warehouses"][wh]["spd"]       = wh_spd


    # ── География покупателей по артикулам из orders ──
    if orders_raw:
        try:
            df_geo = pd.DataFrame(orders_raw)
            c_sku_g  = find_col(df_geo, ["supplierArticle", "sa_name", "article"])
            c_okrug  = find_col(df_geo, ["oblastOkrugName", "oblast_okrug", "okrugName"])
            c_date_g = find_col(df_geo, ["date", "orderDate", "lastChangeDate"])
            c_cancel_g = find_col(df_geo, ["isCancel", "cancel", "cancelled"])

            # Фильтруем отменённые заказы — так же как при расчёте локализации
            if c_cancel_g:
                df_geo = df_geo[df_geo[c_cancel_g] == False]

            if c_sku_g and c_okrug and c_date_g and main_date_from:
                df_geo[c_date_g] = pd.to_datetime(df_geo[c_date_g], errors="coerce", utc=True).dt.tz_localize(None)
                cutoff = pd.to_datetime(main_date_from)
                df_geo = df_geo[df_geo[c_date_g] >= cutoff]

            if c_sku_g and c_okrug and len(df_geo):
                for sku_val, grp in df_geo.groupby(c_sku_g):
                    sku = str(sku_val)
                    if sku not in result["skus"]:
                        continue
                    # Считаем заказы по округам покупателей
                    okrug_counts = grp[c_okrug].value_counts().to_dict()
                    result["skus"][sku]["buyer_regions"] = {
                        str(k): int(v) for k, v in okrug_counts.items() if k and str(k).strip()
                    }

            # ── Потребность по складам на основе regionName ──
            # regionName = конкретный субъект РФ ("Московская", "Удмуртская" и т.д.)
            # Маппим каждый субъект на ближайший наш склад через REGION_TO_WAREHOUSE
            c_region = find_col(df_geo, ["regionName", "region_name", "region"])
            if c_sku_g and c_region and len(df_geo):
                for sku_val, grp in df_geo.groupby(c_sku_g):
                    sku = str(sku_val)
                    if sku not in result["skus"]:
                        continue
                    # Считаем заказы по субъектам
                    region_counts = grp[c_region].value_counts().to_dict()
                    result["skus"][sku]["buyer_subjects"] = {
                        str(k): int(v) for k, v in region_counts.items() if k and str(k).strip()
                    }
                    # Переводим субъекты → потребность на каждый наш склад
                    wh_demand = {}
                    unmatched = 0
                    for region, cnt in region_counts.items():
                        region_str = str(region).strip()
                        if not region_str:
                            continue
                        # Ищем склад: точное совпадение, затем частичное
                        wh = REGION_TO_WAREHOUSE.get(region_str)
                        if not wh:
                            # Частичное совпадение (WB может отдавать разные форматы)
                            for key, val in REGION_TO_WAREHOUSE.items():
                                if key.lower() in region_str.lower() or region_str.lower() in key.lower():
                                    wh = val
                                    break
                        if wh:
                            wh_demand[wh] = wh_demand.get(wh, 0) + int(cnt)
                        else:
                            unmatched += int(cnt)
                    # Если есть нераспознанные — раскидываем пропорционально известным
                    if unmatched > 0 and wh_demand:
                        total_matched = sum(wh_demand.values())
                        for wh in wh_demand:
                            wh_demand[wh] += round(unmatched * wh_demand[wh] / total_matched)
                    result["skus"][sku]["wh_demand"] = wh_demand
        except Exception as e:
            print(f"География покупателей: {e}")

    # ── Процент выкупа = неотменённые заказы / все заказы ──
    if orders_raw:
        try:
            df_ord = pd.DataFrame(orders_raw)
            c_sku_o  = find_col(df_ord, ["supplierArticle", "sa_name", "article"])
            c_cancel = find_col(df_ord, ["isCancel", "cancel", "cancelled"])
            c_date_o = find_col(df_ord, ["date", "orderDate", "lastChangeDate"])

            if c_sku_o and c_date_o and main_date_from:
                df_ord[c_date_o] = pd.to_datetime(df_ord[c_date_o], errors="coerce", utc=True).dt.tz_localize(None)
                cutoff = pd.to_datetime(main_date_from)
                df_ord2 = df_ord[df_ord[c_date_o] >= cutoff]
            else:
                df_ord2 = df_ord

            if c_sku_o and len(df_ord2):
                for sku_val, grp in df_ord2.groupby(c_sku_o):
                    sku = str(sku_val)
                    total_ord = len(grp)
                    cancelled  = len(grp[grp[c_cancel] == True]) if c_cancel else 0
                    bought     = total_ord - cancelled
                    if total_ord > 0 and sku in result["skus"]:
                        result["skus"][sku]["buyout_rate"] = round(bought / total_ord * 100, 1)
        except Exception as e:
            print(f"Выкуп: {e}")

    # Для артикулов без данных ставим 100%
    missing_dims = []
    for sku, info in result["skus"].items():
        if "buyout_rate" not in info:
            info["buyout_rate"] = 100.0
        # Дозаполняем габариты если пустые (артикул мог создаться через остатки/заказы)
        if not info.get("dims") or not info["dims"].get("volume"):
            nmid = info.get("nmid", "")
            found_dims = dims.get(sku) or dims.get(nmid) or {}
            if found_dims:
                info["dims"] = found_dims
            else:
                missing_dims.append(f"{sku} (nmid:{nmid})")

    if missing_dims:
        print(f"Габариты не найдены для {len(missing_dims)} артикулов: {', '.join(missing_dims[:10])}{'...' if len(missing_dims) > 10 else ''}")
    else:
        print(f"Габариты заполнены для всех {len(result['skus'])} артикулов")

    # ── Расчёт средней логистики по каждому артикулу ──
    # Средневзвешенная по реальным продажам со складов
    # КТР=1, ИРП=0 — базовые тарифы, чтобы сравнение с прогнозом было корректным
    wh_params = get_default_wh_params()
    for sku, info in result["skus"].items():
        volume_l   = info.get("dims", {}).get("volume", 1.0) or 1.0
        buyout_rate = info.get("buyout_rate", 100.0)

        # Считаем средневзвешенную по складам (по заказам с каждого склада)
        total_orders_wh = 0
        weighted_cost   = 0

        for wh_name, wh_data in info.get("warehouses", {}).items():
            orders = wh_data.get("orders", 0)
            if orders <= 0:
                continue
            par = wh_params.get(wh_name, {})
            if not par:
                continue
            log_first = par.get("logistics_first", 46)
            log_extra = par.get("logistics_extra", 14)
            wh_coef   = par.get("wb_logistics_coef", 1.0)

            # КТР=1, ИРП=0 — базовый тариф склада + обратная логистика
            cost = calc_logistics_cost(volume_l, log_first, log_extra, wh_coef, 1.0, 0, 0.0, buyout_rate)
            weighted_cost   += cost * orders
            total_orders_wh += orders

        if total_orders_wh > 0:
            info["avg_logistics"] = round(weighted_cost / total_orders_wh, 1)
        else:
            avg_first = 75.0
            avg_extra = 23.0
            info["avg_logistics"] = round(calc_logistics_cost(volume_l, avg_first, avg_extra, 1.0, 1.0, 0, 0.0, buyout_rate), 1)

    result["warehouses"] = sorted(list(result["warehouses"]))

    # ── Подтягиваем сохранённые параметры из SQLite ──
    saved = load_saved_params()
    for sku, info in result["skus"].items():
        if sku in saved:
            sp = saved[sku]
            # Восстанавливаем параметры если они не пришли из CSV (cost=0 значит не задана)
            if info.get("cost", 0) == 0 and sp.get("cost", 0) > 0:
                info["cost"] = sp["cost"]
            if sp.get("archived"):
                info["archived"] = True
            if sp.get("source") and sp["source"] != "Россия":
                info["source"] = sp["source"]
            if sp.get("lead_days") and sp["lead_days"] != 2:
                info["lead_days"] = sp["lead_days"]
            if sp.get("box_qty") and sp["box_qty"] != 1:
                info["box_qty"] = sp["box_qty"]
            if sp.get("ff_stock") and sp["ff_stock"] > 0:
                info["ff_stock"] = sp["ff_stock"]

    return result

# ── 3. Получить данные для UI ──
@app.route("/api/get_skus")
def get_skus():
    data = get_store().get("data")
    if not data:
        return jsonify({"ok": False, "error": "Данные не загружены"})
    skus_list = []
    for sku, info in data["skus"].items():
        active_days = info.get("active_days", get_store().get("days", 30))
        spd = info["sales"] / active_days if active_days > 0 else 0
        
        # Темп по заказам (для планирования)
        orders_count = info.get("orders_count", 0)
        active_days_orders = info.get("active_days_orders", active_days)
        spd_orders = orders_count / active_days_orders if active_days_orders > 0 else 0
        
        # Выкуп из process_data (неотменённые / все заказы)
        buyout_auto = info.get("buyout_rate", 100.0)
        
        # Продажи = заказы × выкуп%
        sales_calc = round(orders_count * buyout_auto / 100)
        # Темп продаж = темп заказов × выкуп%
        spd_sales_calc = round(spd_orders * buyout_auto / 100, 2)
        
        skus_list.append({
            "sku":       sku,
            "nmid":      info.get("nmid", ""),
            "name":      info["name"],
            "sales":     sales_calc,
            "orders_count": orders_count,
            "sales_prev":info.get("sales_prev", 0),
            "spd":       round(spd_orders, 2),  # темп по заказам для планирования
            "spd_sales": spd_sales_calc,          # темп по продажам = spd × выкуп%
            "active_days": active_days_orders,
            "stock_total": info["stock_total"],
            "in_way_to_wh": info.get("in_way_to_wh", 0),
            "days_left": round(info["stock_total"] / spd_orders, 0) if spd_orders > 0 else 999,
            "cost": info.get("cost", 0),
            "dims": info.get("dims", {}),
            "volume_l": info.get("dims", {}).get("volume", 0),
            "warehouses": info["warehouses"],
            "archived":   info.get("archived", False),
            "source":     info.get("source", "Россия"),
            "lead_days":    info.get("lead_days") or (30 if info.get("source","Россия")=="Китай" else 2),
            "buyout_rate":  buyout_auto,
            "returns":      info.get("returns", 0),
            "buyer_regions":    info.get("buyer_regions", {}),
            "localization_pct": info.get("localization_pct", None),
            "local_orders_cnt": info.get("local_orders_cnt", 0),
            "total_orders_loc": info.get("total_orders_loc", 0),
            "box_qty":    info.get("box_qty", 1) or 1,
            "ff_stock":   info.get("ff_stock", 0),
            "wh_demand":  info.get("wh_demand", {}),
            "buyer_subjects": info.get("buyer_subjects", {}),
            "avg_logistics": info.get("avg_logistics", 0),
            "trend": info.get("trend", {"coef": 1.0, "direction": "stable", "reliable": False, "weeks": []}),
        })
    skus_list.sort(key=lambda x: x["days_left"])
    return jsonify({"ok": True, "skus": skus_list, "warehouses": data["warehouses"]})

# ── 4. Сохранить себестоимость и габариты ──
@app.route("/api/save_sku_params", methods=["POST"])
def save_sku_params():
    params = request.json.get("params", {})
    data = get_store().get("data", {})
    db_params = {}
    for sku, p in params.items():
        if sku in data.get("skus", {}):
            data["skus"][sku]["cost"]     = float(p.get("cost", 0))
            data["skus"][sku]["archived"] = bool(p.get("archived", False))
            source = str(p.get("source", "Россия"))
            data["skus"][sku]["source"]   = source
            default_lead = 30 if source == "Китай" else 2
            data["skus"][sku]["lead_days"] = int(p.get("lead_days", default_lead))
            data["skus"][sku]["box_qty"]   = int(p.get("box_qty", 1)) or 1
            data["skus"][sku]["ff_stock"]  = int(p.get("ff_stock", 0))
            if "dims" in p:
                data["skus"][sku]["dims"] = p["dims"]
            # Собираем для сохранения в SQLite
            db_params[sku] = {
                "cost": data["skus"][sku]["cost"],
                "archived": data["skus"][sku].get("archived", False),
                "source": source,
                "lead_days": data["skus"][sku]["lead_days"],
                "box_qty": data["skus"][sku]["box_qty"],
                "ff_stock": data["skus"][sku]["ff_stock"],
            }
    get_store()["data"] = data
    # Сохраняем в SQLite
    if db_params:
        save_params_to_db(db_params)
    return jsonify({"ok": True})

# ── 5. Сохранить параметры складов ──
@app.route("/api/save_warehouse_params", methods=["POST"])
def save_warehouse_params():
    get_store()["wh_params"] = request.json.get("params", {})
    return jsonify({"ok": True})

# ── 5a. Загрузка реальной логистики из финотчёта ──
@app.route("/api/load_logistics", methods=["POST"])
def load_logistics():
    token = get_store().get("token")
    if not token:
        return jsonify({"ok": False, "error": "Нет токена"})
    data = get_store().get("data")
    if not data:
        return jsonify({"ok": False, "error": "Сначала загрузите данные"})
    
    days = get_store().get("days", 28)
    date_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    date_to = datetime.now().strftime("%Y-%m-%d")
    
    report = fetch_report_logistics(token, date_from, date_to)
    if not report:
        return jsonify({"ok": False, "error": "Не удалось загрузить отчёт реализации"})
    
    updated = 0
    for sku, info in data["skus"].items():
        if sku in report:
            info["avg_logistics"] = report[sku]
            updated += 1
    
    get_store()["data"] = data
    return jsonify({"ok": True, "updated": updated, "total": len(report)})

# ── 5b. Импорт цен и остатков из Excel/CSV ──
@app.route("/api/import_prices", methods=["POST"])
def import_prices():
    try:
        file = request.files.get("file")
        if not file:
            return jsonify({"ok": False, "error": "Файл не загружен"})

        filename = file.filename.lower()
        rows = []

        if filename.endswith(('.xlsx', '.xls')):
            import openpyxl
            from io import BytesIO
            wb = openpyxl.load_workbook(BytesIO(file.read()), read_only=True, data_only=True)
            ws = wb.active
            for row in ws.iter_rows(values_only=True):
                rows.append(list(row))
            wb.close()
        elif filename.endswith('.csv'):
            import csv
            from io import StringIO
            text = file.read().decode('utf-8', errors='ignore')
            reader = csv.reader(StringIO(text))
            for row in reader:
                rows.append(row)
        else:
            return jsonify({"ok": False, "error": "Поддерживаются только .xlsx и .csv"})

        if len(rows) < 2:
            return jsonify({"ok": False, "error": "Файл пустой"})

        # Структура: [0]=Артикул, [1]=Себестоимость, [2]=Остатки на ФФ, [3]=Кратность
        data = get_store().get("data", {})
        skus = data.get("skus", {})
        matched = 0
        not_found = 0
        updated = []

        for row in rows[1:]:
            if not row or len(row) < 3:
                continue

            # [0] Артикул продавца — информационный, пропускаем
            # [1] Артикул WB (nmid)
            nmid = ''.join(c for c in str(row[1]) if c.isdigit()).strip()
            if not nmid:
                continue

            # [2] Себестоимость
            price = 0
            if len(row) > 2 and row[2] is not None:
                price_str = str(row[2]).replace('р.', '').replace('₽', '').replace('\xa0', '').replace(' ', '').replace(',', '.').strip()
                try:
                    price = float(price_str) if price_str else 0
                except:
                    price = 0

            # [3] Остатки на ФФ (если пусто → 0)
            ff_stock = 0
            if len(row) > 3 and row[3] is not None:
                ff_str = ''.join(c for c in str(row[3]) if c.isdigit())
                ff_stock = int(ff_str) if ff_str else 0

            # [4] Кратность (если пусто → 1)
            box_qty = 1
            if len(row) > 4 and row[4] is not None:
                bq_str = ''.join(c for c in str(row[4]) if c.isdigit())
                box_qty = int(bq_str) if bq_str and int(bq_str) > 0 else 1

            # Ищем артикул по nmid
            found_sku = None
            for sku, info in skus.items():
                if str(info.get("nmid", "")) == nmid:
                    found_sku = sku
                    break

            if found_sku:
                if price > 0:
                    skus[found_sku]["cost"] = price
                skus[found_sku]["ff_stock"] = ff_stock
                skus[found_sku]["box_qty"] = box_qty
                updated.append({"nmid": nmid, "cost": price, "ff_stock": ff_stock, "box_qty": box_qty})
                matched += 1
                # Сохраняем в SQLite
                save_params_to_db({found_sku: {
                    "cost": price if price > 0 else skus[found_sku].get("cost", 0),
                    "ff_stock": ff_stock,
                    "box_qty": box_qty,
                    "archived": skus[found_sku].get("archived", False),
                    "source": skus[found_sku].get("source", "Россия"),
                    "lead_days": skus[found_sku].get("lead_days", 2),
                }})
            else:
                not_found += 1

        get_store()["data"] = data
        return jsonify({"ok": True, "matched": matched, "not_found": not_found, "updated": updated})

    except Exception as e:
        import traceback
        print("import_prices error:", traceback.format_exc())
        return jsonify({"ok": False, "error": str(e)})

# ── 5c. Загрузить реальную логистику из финотчёта WB ──
@app.route("/api/load_report_logistics", methods=["POST"])
def load_report_logistics():
    token = get_store().get("token")
    if not token:
        return jsonify({"ok": False, "error": "Нет токена"})
    data = get_store().get("data")
    if not data:
        return jsonify({"ok": False, "error": "Сначала загрузите данные"})
    
    # Берём последние 7 дней — достаточно для средней логистики, грузится быстро
    date_from_r = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    date_to_r = datetime.now().strftime("%Y-%m-%d")
    
    try:
        report = fetch_report_logistics(token, date_from_r, date_to_r)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Ошибка загрузки: {e}"})
    
    if not report:
        return jsonify({"ok": False, "error": "Отчёт реализации пуст или недоступен"})
    
    # Подставляем реальную логистику в данные
    updated = 0
    for sku, info in data["skus"].items():
        if sku in report:
            info["avg_logistics"] = report[sku]
            updated += 1
    
    get_store()["data"] = data
    return jsonify({"ok": True, "updated": updated, "total": len(report)})

# ── 6. Рассчитать план закупок + оптимальные склады ──
@app.route("/api/calc_plan", methods=["POST"])
def calc_plan():
    data = get_store().get("data")
    if not data:
        return jsonify({"ok": False, "error": "Нет данных"})

    body        = request.json
    horizon     = int(body.get("horizon", 30))       # дней планирования
    lead_time   = int(body.get("lead_time", 14))      # логистическое плечо до склада WB
    safety_days = int(body.get("safety_days", 3))  # страховой запас в днях
    growth_pct  = float(body.get("growth_pct", 0))   # прогноз роста %
    min_order   = int(body.get("min_order", 1))
    wh_params   = get_store().get("wh_params", {})
    days        = get_store().get("days", 30)
    # Кол-во дней в предыдущем периоде (для тренда) — всегда 14
    trend_days  = data.get("trend_extra", 14)

    plan = []

    for sku, info in data["skus"].items():
        if info.get("archived", False):
            continue  # скрытый товар — не включаем в план
        # Фильтр: меньше 30 заказов за период — не планируем
        if info.get("orders_count", info.get("sales", 0)) < 30:
            continue
        # Базовый темп — по заказам (от первого заказа)
        active_days = info.get("active_days_orders", info.get("active_days", days))
        orders_count = info.get("orders_count", info.get("sales", 0))
        spd_curr = orders_count / active_days if active_days > 0 else 0
        # Применяем тренд если устойчивый
        trend_info = info.get("trend", {})
        trend_coef = trend_info.get("coef", 1.0) if trend_info.get("reliable") else 1.0
        # Применяем сезонность
        subject = info.get("dims", {}).get("subject", "")
        now = datetime.now()
        cur_month = now.strftime('%m')
        next_month = (now.replace(day=28) + timedelta(days=4)).strftime('%m')
        season_mult, season_coefs = get_season_multiplier(subject, cur_month, next_month)
        spd = round(spd_curr * trend_coef * season_mult, 2)
        stock = info["stock_total"]
        days_left = round(stock / spd, 0) if spd > 0 else 999

        # Плечо берём из настроек товара (задаётся в разделе Товары)
        # Россия = 2 дня по умолчанию, Китай = 30 дней по умолчанию
        source = info.get("source", "Россия")
        default_lead  = 30 if source == "Китай" else 2
        supplier_lead = int(info.get("lead_days", default_lead))   # индивидуальное плечо товара
        wb_lead       = 0                                          # плечо до WB включено в supplier_lead
        total_lead    = supplier_lead

        # Нужно на горизонт + плечо поставки + страховой запас
        # ff_stock — уже закупленный товар на нашем складе, готов к отгрузке
        ff_stock = int(info.get("ff_stock", 0))
        needed   = math.ceil(spd * (horizon + total_lead + safety_days))
        to_order = max(0, needed - stock - ff_stock)
        # Округляем по кратности (box_qty)
        box_qty = int(info.get("box_qty", 1)) or 1
        if to_order > 0 and box_qty > 1:
            to_order = math.ceil(to_order / box_qty) * box_qty
        if 0 < to_order < min_order:
            to_order = min_order

        # Статус по фиксированным порогам остатка в днях:
        # < 7 дней  → срочно (danger)
        # 7–14 дней → скоро (warn)
        # 14–30 дней → в норме (ok)
        # > 30 дней  → пересорт (overstock)
        if days_left < 7:
            status = purchase_urgency = "danger"
        elif days_left < 14:
            status = purchase_urgency = "warn"
        elif days_left <= 30:
            status = purchase_urgency = "ok"
        else:
            status = purchase_urgency = "overstock"

        if spd > 0 and days_left < 999:
            # Закупить до = за 5 дней до окончания остатков
            purchase_by_days = max(0, int(days_left) - 5)
            purchase_by = (datetime.now() + timedelta(days=purchase_by_days)).strftime("%d.%m")

            # Отгрузить до = дата закупки + 2 дня
            order_by_days = purchase_by_days + 2
            order_by = (datetime.now() + timedelta(days=order_by_days)).strftime("%d.%m")
        else:
            order_by = purchase_by = "—"

        # ── Оптимальное распределение по складам ──
        wh_distribution = calc_wh_distribution(sku, info, to_order, wh_params, spd)

        # Не включаем в план если нечего заказывать
        if to_order == 0:
            continue

        plan.append({
            "sku":              sku,
            "nmid":             info.get("nmid", ""),
            "name":             info["name"],
            "source":           info.get("source", "Россия"),
            "lead_days":        supplier_lead,
            "box_qty":          int(info.get("box_qty", 1)) or 1,
            "buyout_rate":      info.get("buyout_rate", 100.0),
            "ff_stock":         ff_stock,
            "spd":              spd,
            "spd_raw":          round(spd_curr, 2),
            "trend_coef":       trend_coef,
            "trend_direction":  trend_info.get("direction", "stable"),
            "trend_reliable":   trend_info.get("reliable", False),
            "season_mult":      season_mult,
            "season_subject":   subject,
            "stock":            stock,
            "days_left":        int(min(days_left, 999)),
            "to_order":         to_order,
            "cost":             info.get("cost", 0),
            "total_cost":       round(to_order * info.get("cost", 0), 0),
            "status":           status,
            "purchase_urgency": purchase_urgency,
            "purchase_by":      purchase_by if to_order > 0 else "—",
            "order_by":         order_by if to_order > 0 else "—",
            "dims":             info.get("dims", {}),
            "wh_distribution":  wh_distribution,
        })

    plan.sort(key=lambda x: x["days_left"])

    # Итоги
    totals = {
        "danger":       sum(1 for p in plan if p["status"] == "danger"),
        "warn":         sum(1 for p in plan if p["status"] == "warn"),
        "overstock":    sum(1 for p in plan if p["status"] == "overstock"),
        "to_order_qty": sum(p["to_order"] for p in plan),
        "to_order_boxes": sum(math.ceil(p["to_order"] / max(p.get("box_qty", 1), 1)) for p in plan if p["to_order"] > 0),
        "to_order_sum": sum(p["total_cost"] for p in plan),
    }

    return jsonify({"ok": True, "plan": plan, "totals": totals})

def calc_wh_distribution(sku, info, to_order, wh_params, spd):
    """
    Оптимальное распределение товара по складам.
    Балансирует: % локализации + стоимость хранения WB + коэф логистики WB.
    """
    if to_order == 0:
        return []

    warehouses = info.get("warehouses", {})
    if not warehouses:
        return []

    scored = []
    for wh, wh_data in warehouses.items():
        p = wh_params.get(wh, {})
        storage_cost = float(p.get("storage_cost", 2.0))    # ₽/шт/день
        wb_logistics = float(p.get("wb_logistics_coef", 1.0))  # коэф логистики WB
        return_rate  = float(p.get("return_rate", 10.0))    # % возвратов
        accepts      = bool(p.get("accepts", True))

        if not accepts:
            continue

        local_pct  = wh_data.get("local_pct", 0)
        stock      = wh_data.get("stock", 0)
        orders     = wh_data.get("orders", 0)

        if local_pct == 0 and orders == 0:
            continue

        # Дней продаж на складе
        wh_spd = spd * local_pct / 100 if local_pct > 0 else 0
        days_of_stock = round(stock / wh_spd, 0) if wh_spd > 0 else 999

        # Оценка склада (выше = лучше)
        # Локализация: главный фактор (40%)
        loc_score = local_pct  # 0..100

        # Стоимость хранения: дешевле — лучше (30%)
        # Нормируем: 0 ₽ = 100 очков, 5 ₽ = 0 очков
        cost_score = max(0, 100 - storage_cost * 20)

        # Логистика WB: ниже коэф — лучше (20%)
        log_score = max(0, 100 - (wb_logistics - 1) * 50)

        # Возвраты: меньше — лучше (10%)
        ret_score = max(0, 100 - return_rate * 2)

        total_score = round(
            loc_score * 0.40 +
            cost_score * 0.30 +
            log_score * 0.20 +
            ret_score * 0.10, 1
        )

        scored.append({
            "warehouse":      wh,
            "local_pct":      local_pct,
            "stock":          stock,
            "days_of_stock":  int(min(days_of_stock, 999)),
            "storage_cost":   storage_cost,
            "wb_logistics":   wb_logistics,
            "score":          total_score,
            "orders":         orders,
        })

    if not scored:
        return []

    scored.sort(key=lambda x: x["score"], reverse=True)

    # Распределяем пропорционально score × local_pct
    total_weight = sum(s["score"] * s["local_pct"] for s in scored if s["local_pct"] > 0)
    if total_weight == 0:
        total_weight = sum(s["score"] for s in scored)
        for s in scored:
            s["weight"] = s["score"]
    else:
        for s in scored:
            s["weight"] = s["score"] * s["local_pct"]

    remaining = to_order
    for i, s in enumerate(scored):
        if i == len(scored) - 1:
            s["qty"] = remaining
        else:
            s["qty"] = max(0, round(to_order * s["weight"] / total_weight))
            remaining -= s["qty"]
        s["qty"] = max(0, s["qty"])

    return [s for s in scored if s["qty"] > 0 or s["local_pct"] > 0]

# ── 7. Получить параметры складов ──
@app.route("/api/get_warehouse_params")
def get_warehouse_params():
    data = get_store().get("data", {})
    wh_params = get_store().get("wh_params", {})
    warehouses = data.get("warehouses", [])

    # Актуальные тарифы WB на 23.03.2026
    # logistics_first/extra — стоимость логистики WB за 1й и доп. литр (руб)
    # storage_cost — хранение руб/шт/день
    # wb_logistics_coef — нормированный коэф (Невинномысск=1.0)
    # accepts: False = приёмка недоступна
    defaults = {
        # Тарифы по скриншотам ЛК WB 25.03.2026
        # logistics_first/extra — стоимость логистики ₽ за 1й и доп. литр (УЖЕ с коэф. склада)
        # storage_cost — хранение ₽/литр/день (первый/доп.л — одинаковые на скриншотах)
        "Невинномысск":               {"storage_cost": 0.08, "logistics_first": 52.9,  "logistics_extra": 16.1, "wb_logistics_coef": 1.15, "return_rate": 10, "lead_days": 2, "accepts": True},
        "Тула":                       {"storage_cost": 0.13, "logistics_first": 73.6,  "logistics_extra": 22.4, "wb_logistics_coef": 1.60, "return_rate": 10, "lead_days": 1, "accepts": True},
        "Самара (Новосемейкино)":      {"storage_cost": 0.14, "logistics_first": 75.9,  "logistics_extra": 23.1, "wb_logistics_coef": 1.65, "return_rate": 10, "lead_days": 2, "accepts": True},
        "Краснодар":                   {"storage_cost": 0.13, "logistics_first": 75.9,  "logistics_extra": 23.1, "wb_logistics_coef": 1.65, "return_rate": 10, "lead_days": 2, "accepts": True},
        "Сарапул":                    {"storage_cost": 0.14, "logistics_first": 80.5,  "logistics_extra": 24.5, "wb_logistics_coef": 1.75, "return_rate": 10, "lead_days": 2, "accepts": True},
        "Электросталь":               {"storage_cost": 0.14, "logistics_first": 82.8,  "logistics_extra": 25.2, "wb_logistics_coef": 1.80, "return_rate": 10, "lead_days": 1, "accepts": True},
        "Екатеринбург - Перспективная 14": {"storage_cost": 0.14, "logistics_first": 87.4, "logistics_extra": 26.6, "wb_logistics_coef": 1.90, "return_rate": 10, "lead_days": 2, "accepts": True},
        "Коледино":                   {"storage_cost": 0.16, "logistics_first": 94.3,  "logistics_extra": 28.7, "wb_logistics_coef": 2.05, "return_rate": 10, "lead_days": 1, "accepts": True},
        "СПБ Шушары":                 {"storage_cost": 0.18, "logistics_first": 101.2, "logistics_extra": 30.8, "wb_logistics_coef": 2.20, "return_rate": 10, "lead_days": 1, "accepts": True},
        "Казань":                     {"storage_cost": 0.18, "logistics_first": 103.5, "logistics_extra": 31.5, "wb_logistics_coef": 2.25, "return_rate": 10, "lead_days": 2, "accepts": True},
    }

    result = {}
    for wh in warehouses:
        saved = wh_params.get(wh, {})
        default = defaults.get(wh, {"storage_cost": 2.0, "wb_logistics_coef": 1.0,
                                     "return_rate": 10, "lead_days": 2, "accepts": True})
        result[wh] = {**default, **saved}

    return jsonify({"ok": True, "params": result})

# ═══════════════════════════════════════════════════════
# МОДЕЛЬ ОПТИМАЛЬНОГО РАСПРЕДЕЛЕНИЯ ПО СКЛАДАМ
# Цель: ИЛ < 1 (локализация ≥ 60%) и ИРП = 0%
# ═══════════════════════════════════════════════════════

def get_default_wh_params():
    """Возвращает дефолтные параметры складов."""
    return {
        "Невинномысск":               {"storage_cost": 0.08, "logistics_first": 52.9,  "logistics_extra": 16.1, "wb_logistics_coef": 1.15},
        "Тула":                       {"storage_cost": 0.13, "logistics_first": 73.6,  "logistics_extra": 22.4, "wb_logistics_coef": 1.60},
        "Самара (Новосемейкино)":      {"storage_cost": 0.14, "logistics_first": 75.9,  "logistics_extra": 23.1, "wb_logistics_coef": 1.65},
        "Краснодар":                   {"storage_cost": 0.13, "logistics_first": 75.9,  "logistics_extra": 23.1, "wb_logistics_coef": 1.65},
        "Сарапул":                    {"storage_cost": 0.14, "logistics_first": 80.5,  "logistics_extra": 24.5, "wb_logistics_coef": 1.75},
        "Электросталь":               {"storage_cost": 0.14, "logistics_first": 82.8,  "logistics_extra": 25.2, "wb_logistics_coef": 1.80},
        "Екатеринбург - Перспективная 14": {"storage_cost": 0.14, "logistics_first": 87.4, "logistics_extra": 26.6, "wb_logistics_coef": 1.90},
        "Коледино":                   {"storage_cost": 0.16, "logistics_first": 94.3,  "logistics_extra": 28.7, "wb_logistics_coef": 2.05},
        "СПБ Шушары":                 {"storage_cost": 0.18, "logistics_first": 101.2, "logistics_extra": 30.8, "wb_logistics_coef": 2.20},
        "Казань":                     {"storage_cost": 0.18, "logistics_first": 103.5, "logistics_extra": 31.5, "wb_logistics_coef": 2.25},
    }

# Карта: город/регион покупателя → ближайший склад WB
# Источник: данные WB о локализации поставок
CITY_TO_WAREHOUSE = {
    # Центральный ФО
    "Москва Центр":       "Электросталь",
    "Москва Север":       "Электросталь",
    "Москва Запад":       "Коледино",
    "Москва Юг":          "Коледино",
    "Москва Восток":      "Коледино",
    "Коломна":            "Коледино",
    "Орехово-Зуево":      "Электросталь",
    "Балашиха":           "Электросталь",
    "Подольск":           "Коледино",
    "Смоленск":           "Коледино",
    "Тверь":              "Электросталь",
    "Рязань":             "Коледино",
    "Тамбов":             "Тула",
    "Воронеж":            "Тула",
    "Липецк":             "Тула",
    "Курск":              "Тула",
    "Тула":               "Тула",
    "Ярославль":          "Электросталь",
    "Белгород":           "Коледино",
    "Брянск":             "Коледино",
    "Вологда":            "Электросталь",
    "Калининград":        "Коледино",
    # Северо-Западный ФО
    "Санкт-Петербург":    "СПБ Шушары",
    "Санкт-Петербург 2":  "СПБ Шушары",
    "Мурманск":           "СПБ Шушары",
    # Приволжский ФО
    "Тольятти":           "Самара (Новосемейкино)",
    "Саратов":            "Самара (Новосемейкино)",
    "Набережные Челны":   "Казань",
    "Оренбург":           "Самара (Новосемейкино)",
    "Самара":             "Самара (Новосемейкино)",
    "Уфа":                "Сарапул",
    "Казань":             "Казань",
    "Чебоксары":          "Казань",
    "Нижний Новгород":    "Казань",
    "Ульяновск":          "Казань",
    "Пенза":              "Самара (Новосемейкино)",
    "Ижевск":             "Сарапул",
    "Пермь":              "Сарапул",
    "Киров":              "Казань",
    "Саранск":            "Самара (Новосемейкино)",
    # Южный + Северо-Кавказский ФО
    "Волгоград":          "Краснодар",
    "Ростов-на-Дону":     "Краснодар",
    "Астрахань":          "Краснодар",
    "Краснодар":          "Краснодар",
    "Волжский":           "Краснодар",
    "Сочи":               "Краснодар",
    "Махачкала":          "Невинномысск",
    "Ставрополь":         "Невинномысск",
    "Грозный":            "Невинномысск",
    # Уральский ФО
    "Магнитогорск":       "Екатеринбург - Перспективная 14",
    "Тюмень":             "Екатеринбург - Перспективная 14",
    "Челябинск":          "Екатеринбург - Перспективная 14",
    "Екатеринбург":       "Екатеринбург - Перспективная 14",
    "Сургут":             "Екатеринбург - Перспективная 14",
    # Сибирский ФО (нет нашего склада — ближайший Екатеринбург)
    "Омск":               "Екатеринбург - Перспективная 14",
    "Кемерово":           "Екатеринбург - Перспективная 14",
    "Новосибирск":        "Екатеринбург - Перспективная 14",
    "Барнаул":            "Екатеринбург - Перспективная 14",
    "Красноярск":         "Екатеринбург - Перспективная 14",
    "Томск":              "Екатеринбург - Перспективная 14",
    "Новокузнецк":        "Екатеринбург - Перспективная 14",
    "Якутск":             "Екатеринбург - Перспективная 14",
    "Иркутск":            "Екатеринбург - Перспективная 14",
    # Дальневосточный ФО (нет нашего склада — ближайший Екатеринбург)
    "Хабаровск":          "Екатеринбург - Перспективная 14",
    "Благовещенск":       "Екатеринбург - Перспективная 14",
    "Владивосток":        "Екатеринбург - Перспективная 14",
}

# Обратная карта: склад → список городов которые он покрывает
WAREHOUSE_CITIES = {}
for city, wh in CITY_TO_WAREHOUSE.items():
    WAREHOUSE_CITIES.setdefault(wh, []).append(city)

# Маппинг: oblastOkrugName из API → кластер WB
# ВАЖНО: WB считает локализацию по РЕАЛЬНЫМ федеральным округам.
# Покупатель из Сибирского ФО + склад из Уральского ФО = НЕ локальный заказ.
# Поэтому каждый ФО маппится в свой кластер, даже если у нас там нет склада.
BUYER_OKRUG_TO_CLUSTER = {
    "Центральный федеральный округ":        "Центральный",
    "Центральный":                           "Центральный",
    "Южный федеральный округ":               "Южный",
    "Северо-Кавказский федеральный округ":   "Южный",       # WB объединяет с Южным
    "Южный":                                 "Южный",
    "Северо-Кавказский":                     "Южный",
    "Приволжский федеральный округ":         "Приволжский",
    "Приволжский":                           "Приволжский",
    "Уральский федеральный округ":           "Уральский",
    "Уральский":                             "Уральский",
    "Северо-Западный федеральный округ":     "Северо-Западный",
    "Северо-Западный":                       "Северо-Западный",
    "Сибирский федеральный округ":           "Уральский",       # нет нашего склада → Екатеринбург
    "Дальневосточный федеральный округ":     "Уральский",       # нет нашего склада → Екатеринбург
    "Сибирский":                             "Уральский",
    "Дальневосточный":                       "Уральский",
}

# Для обратной совместимости — регион каждого склада
WAREHOUSE_REGION = {
    "Коледино":                        "Центральный",
    "Электросталь":                    "Центральный",
    "Тула":                             "Центральный",
    "Краснодар":          "Южный",
    "Невинномысск":                    "Южный",
    "Самара (Новосемейкино)":                   "Приволжский",
    "Казань":                          "Приволжский",
    "Сарапул":                         "Приволжский",
    "Екатеринбург - Перспективная 14": "Уральский",
    "СПБ Шушары":                    "Северо-Западный",
}

# ═══════════════════════════════════════════════════════
# МАППИНГ: regionName из WB API → ближайший НАШ склад
# Используется для точного поартикульного распределения.
# regionName приходит в формате "Московская", "Пермский" и т.д.
# ═══════════════════════════════════════════════════════
REGION_TO_WAREHOUSE = {
    # ── Центральный ФО (10 складов: Коледино, Электросталь, Тула) ──
    "Московская":       "Коледино",
    "Москва":           "Коледино",
    "Смоленская":       "Коледино",
    "Брянская":         "Коледино",
    "Белгородская":     "Коледино",
    "Калужская":        "Коледино",
    "Тульская":         "Тула",
    "Орловская":        "Тула",
    "Курская":          "Тула",
    "Рязанская":        "Коледино",
    "Тамбовская":       "Тула",
    "Липецкая":         "Тула",
    "Воронежская":      "Тула",
    "Тверская":         "Электросталь",
    "Владимирская":     "Электросталь",
    "Ивановская":       "Электросталь",
    "Ярославская":      "Электросталь",
    "Костромская":      "Электросталь",

    # ── Северо-Западный ФО ──
    "Санкт-Петербург":      "СПБ Шушары",
    "Ленинградская":        "СПБ Шушары",
    "Новгородская":         "СПБ Шушары",
    "Псковская":            "СПБ Шушары",
    "Калининградская":      "СПБ Шушары",
    "Мурманская":           "СПБ Шушары",
    "Архангельская":        "СПБ Шушары",
    "Вологодская":          "СПБ Шушары",
    "Республика Карелия":   "СПБ Шушары",
    "Республика Коми":      "СПБ Шушары",
    "Ненецкий":             "СПБ Шушары",

    # ── Южный ФО (склады: Краснодар, Невинномысск) ──
    "Краснодарский":        "Краснодар",
    "Ростовская":           "Краснодар",
    "Волгоградская":        "Краснодар",
    "Астраханская":         "Краснодар",
    "Республика Адыгея":    "Краснодар",
    "Республика Калмыкия":  "Невинномысск",
    "Республика Крым":      "Краснодар",
    "Севастополь":          "Краснодар",

    # ── Северо-Кавказский ФО (WB объединяет с Южным для ИЛ) ──
    "Ставропольский":           "Невинномысск",
    "Республика Дагестан":      "Невинномысск",
    "Чеченская":                "Невинномысск",
    "Кабардино-Балкарская":     "Невинномысск",
    "Республика Ингушетия":     "Невинномысск",
    "Республика Северная Осетия": "Невинномысск",
    "Карачаево-Черкесская":     "Невинномысск",

    # ── Приволжский ФО ──
    "Республика Татарстан":     "Казань",
    "Чувашская":                "Казань",
    "Республика Марий Эл":      "Казань",
    "Ульяновская":              "Казань",
    "Кировская":                "Казань",
    "Удмуртская":               "Сарапул",
    "Пермский":                 "Сарапул",
    "Республика Башкортостан":  "Сарапул",
    "Самарская":                "Самара (Новосемейкино)",
    "Оренбургская":             "Самара (Новосемейкино)",
    "Саратовская":              "Самара (Новосемейкино)",
    "Пензенская":               "Самара (Новосемейкино)",
    "Нижегородская":            "Казань",
    "Республика Мордовия":      "Самара (Новосемейкино)",

    # ── Уральский ФО ──
    "Свердловская":         "Екатеринбург - Перспективная 14",
    "Челябинская":          "Екатеринбург - Перспективная 14",
    "Тюменская":            "Екатеринбург - Перспективная 14",
    "Курганская":           "Екатеринбург - Перспективная 14",
    "Ханты-Мансийский":     "Екатеринбург - Перспективная 14",
    "Ямало-Ненецкий":       "Екатеринбург - Перспективная 14",

    # ── Сибирский ФО (нет нашего склада — ближайший Екатеринбург) ──
    "Новосибирская":        "Екатеринбург - Перспективная 14",
    "Омская":               "Екатеринбург - Перспективная 14",
    "Красноярский":         "Екатеринбург - Перспективная 14",
    "Алтайский":            "Екатеринбург - Перспективная 14",
    "Кемеровская":          "Екатеринбург - Перспективная 14",
    "Иркутская":            "Екатеринбург - Перспективная 14",
    "Томская":              "Екатеринбург - Перспективная 14",
    "Республика Алтай":     "Екатеринбург - Перспективная 14",
    "Республика Тыва":      "Екатеринбург - Перспективная 14",
    "Республика Хакасия":   "Екатеринбург - Перспективная 14",

    # ── Дальневосточный ФО (нет нашего склада — ближайший Екатеринбург) ──
    "Приморский":           "Екатеринбург - Перспективная 14",
    "Хабаровский":          "Екатеринбург - Перспективная 14",
    "Амурская":             "Екатеринбург - Перспективная 14",
    "Сахалинская":          "Екатеринбург - Перспективная 14",
    "Еврейская":            "Екатеринбург - Перспективная 14",
    "Камчатский":           "Екатеринбург - Перспективная 14",
    "Магаданская":          "Екатеринбург - Перспективная 14",
    "Чукотский":            "Екатеринбург - Перспективная 14",
    "Республика Бурятия":   "Екатеринбург - Перспективная 14",
    "Республика Саха":      "Екатеринбург - Перспективная 14",
    "Забайкальский":        "Екатеринбург - Перспективная 14",
}

# Таблица КТР (с 23.03.2026) и КРП по доле локализации
LOCALIZATION_TABLE = [
    (95, 100, 0.50, 0.00),
    (90,  95, 0.60, 0.00),
    (85,  90, 0.70, 0.00),
    (80,  85, 0.80, 0.00),
    (75,  80, 0.90, 0.00),
    (60,  75, 1.00, 0.00),  # ИРП = 0 если >= 60%
    (55,  60, 1.05, 2.00),
    (50,  55, 1.10, 2.05),
    (45,  50, 1.20, 2.05),
    (40,  45, 1.30, 2.10),
    (35,  40, 1.40, 2.10),
    (30,  35, 1.50, 2.15),
    (25,  30, 1.55, 2.20),
    (20,  25, 1.60, 2.25),
    (15,  20, 1.70, 2.30),
    (10,  15, 1.75, 2.35),
    ( 5,  10, 1.80, 2.45),
    ( 0,   5, 2.00, 2.50),
]

def get_ktr_krp(local_pct):
    """Возвращает (КТР, КРП) для заданного % локализации."""
    for lo, hi, ktr, krp in LOCALIZATION_TABLE:
        if lo <= local_pct < hi:
            return ktr, krp
    return 2.00, 2.50

def calc_logistics_cost(volume_l, logistics_first, logistics_extra, wh_coef, il, price, irp_pct, buyout_rate=100):
    """
    Полная стоимость логистики с учётом обратной логистики и % выкупа.
    
    Прямая: (лог_1л + лог_доп) × ИЛ + цена × ИРП
      где лог_1л/доп — тарифы склада (УЖЕ с коэф. склада)
    
    Обратная: (46₽ × min(V,1) + 14₽ × max(V-1,0)) — БАЗОВЫЕ тарифы без коэф склада и ИЛ
    
    Итого на 1 заказ = прямая + обратная × (1 - выкуп/100)
    """
    # Базовые тарифы WB (без коэф. склада) — для обратной логистики
    BASE_FIRST = 46.0
    BASE_EXTRA = 14.0

    v1   = min(volume_l, 1.0)
    vext = max(volume_l - 1.0, 0.0)

    # Прямая — тарифы склада (уже с коэф) × ИЛ + цена × ИРП
    base_direct = logistics_first * v1 + logistics_extra * vext
    direct = base_direct * il + price * (irp_pct / 100)

    # Обратная — базовые тарифы без коэф склада и ИЛ
    base_reverse = BASE_FIRST * v1 + BASE_EXTRA * vext
    reverse = base_reverse

    buyout_frac = buyout_rate / 100
    # Выкуп → платим только прямую. Возврат → платим прямую + обратную
    total = direct + reverse * (1 - buyout_frac)
    return round(total, 2)


@app.route("/api/recommend_warehouses", methods=["POST"])
def recommend_warehouses():
    """
    Правильный расчёт по официальной формуле WB:

    1. Доля локализации артикула = локальные_заказы / все_заказы × 100%
    2. Локальный заказ = покупатель и склад в ОДНОМ федеральном округе
    3. Из oblastOkrugName знаем округ каждого покупателя
    4. Для каждого ФО выбираем наши склады → покупатели получают товар из своего ФО
    5. to_order делим пропорционально покупателям по ФО
    """
    try:
        data    = get_store().get("data")
        wh_par  = get_store().get("wh_params") or {}
        if not wh_par:
            resp = get_warehouse_params()
            wh_par = resp.get_json().get("params", {})
        if not data:
            return jsonify({"ok": False, "error": "Нет данных — загрузите данные во вкладке Подключение"})

        body         = request.json or {}
        days         = get_store().get("days", 14)
        horizon      = int(body.get("horizon", days))   # горизонт = период загрузки
        safety_days  = int(body.get("safety_days", 3))   # страховой запас
        to_order_map = body.get("to_order", {})

        # Склады сгруппированные по кластерам WB
        # Включаем все склады из WAREHOUSE_REGION для которых есть тарифы
        wh_by_cluster = {}
        for wh_name, cluster in WAREHOUSE_REGION.items():
            par = wh_par.get(wh_name, {})
            if not par:
                continue
            if cluster not in wh_by_cluster:
                wh_by_cluster[cluster] = []
            wh_by_cluster[cluster].append({
                "warehouse":         wh_name,
                "logistics_first":   par.get("logistics_first", 46),
                "logistics_extra":   par.get("logistics_extra", 14),
                "wb_logistics_coef": par.get("wb_logistics_coef", 1.0),
                "storage_cost":      par.get("storage_cost", 0.1),
            })
        # Не сортируем по цене — внутри кластера порядок определяется продажами по артикулу

        recommendations = []

        for sku, info in data["skus"].items():
            if info.get("archived"):
                continue

            active_days_r = info.get("active_days_orders", info.get("active_days", days))
            orders_count_r = info.get("orders_count", info.get("sales", 0))
            spd         = orders_count_r / active_days_r if active_days_r > 0 else 0
            # Применяем тренд если устойчивый
            trend_info  = info.get("trend", {})
            trend_coef  = trend_info.get("coef", 1.0) if trend_info.get("reliable") else 1.0
            # Применяем сезонность
            subject_r   = info.get("dims", {}).get("subject", "")
            now_r       = datetime.now()
            cur_m       = now_r.strftime('%m')
            nxt_m       = (now_r.replace(day=28) + timedelta(days=4)).strftime('%m')
            season_m, _ = get_season_multiplier(subject_r, cur_m, nxt_m)
            spd_trended = spd * trend_coef * season_m
            volume_l    = info.get("dims", {}).get("volume", 1.0) or 1.0
            price       = info.get("retail_price", 0) or info.get("cost", 0)  # цена продажи для ИРП
            buyout_rate = info.get("buyout_rate", 100)
            box_qty     = int(info.get("box_qty", 1)) or 1

            # to_order из плана если есть, иначе считаем сами:
            # нужно = spd × (горизонт + плечо), вычитаем остатки
            if sku in to_order_map:
                to_order = int(to_order_map[sku])
            else:
                lead_days = int(info.get("lead_days", 2))
                needed    = math.ceil(spd_trended * (horizon + lead_days))
                stock     = info.get("stock_total", 0)
                ff_stock  = int(info.get("ff_stock", 0))
                to_order  = max(0, needed - stock - ff_stock)
                if box_qty > 1:
                    to_order = math.ceil(to_order / box_qty) * box_qty

            # К отгрузке на склады WB = заказ у поставщика + часть ФФ (только сколько нужно)
            # needed_total = сколько всего нужно на горизонт
            # stock = сколько уже на WB
            # deficit = сколько нужно дополнительно = needed_total - stock
            # ff_to_ship = сколько берём с ФФ = min(ff_stock, deficit)
            # to_ship = to_order + ff_to_ship
            ff_stock     = int(info.get("ff_stock", 0))
            stock        = info.get("stock_total", 0)
            lead_days_s  = int(info.get("lead_days", 2))
            needed_total = math.ceil(spd_trended * (horizon + lead_days_s + safety_days))
            deficit      = max(0, needed_total - stock)
            ff_to_ship   = min(ff_stock, deficit)
            to_ship      = to_order + ff_to_ship

            # Фильтр: только артикулы с галочкой в плане ИЛИ с остатком на ФФ
            in_plan = sku in to_order_map and int(to_order_map.get(sku, 0)) > 0
            has_ff  = int(info.get("ff_stock", 0)) > 0
            if not in_plan and not has_ff:
                continue

            # Округа покупателей из oblastOkrugName
            buyer_regions = info.get("buyer_regions", {})
            if not buyer_regions:
                continue

            # Переводим в наши кластеры
            cluster_buyers = {}
            total_buyers   = 0
            for okrug, cnt in buyer_regions.items():
                cluster = BUYER_OKRUG_TO_CLUSTER.get(okrug)
                if not cluster:
                    # Частичное совпадение
                    for key, val in BUYER_OKRUG_TO_CLUSTER.items():
                        if key.lower() in okrug.lower() or okrug.lower() in key.lower():
                            cluster = val
                            break
                if cluster:
                    cluster_buyers[cluster] = cluster_buyers.get(cluster, 0) + cnt
                    total_buyers += cnt

            if total_buyers == 0:
                continue

            # Текущие остатки по кластерам (уже лежит на складах)
            current_stock_by_cluster = {}
            for wh_name, wh_data in info.get("warehouses", {}).items():
                stk = wh_data.get("stock", 0)
                if stk > 0:
                    cl = WAREHOUSE_REGION.get(wh_name)
                    if cl:
                        current_stock_by_cluster[cl] = current_stock_by_cluster.get(cl, 0) + stk

            # Строим рекомендацию: для каждого кластера покупателей → склады в этом кластере
            # Внутри кластера склады делятся пропорционально реальным продажам с каждого склада
            # Данные всех складов артикула — нужны для расчёта локализации по складам
            all_wh_orders    = info.get("warehouses", {})
            total_all_orders = info.get("total_orders_loc", sum(d.get("orders", 0) for d in all_wh_orders.values()))

            distribution  = []
            local_buyers  = 0

            sorted_clusters = sorted(cluster_buyers.items(), key=lambda x: x[1], reverse=True)

            # Считаем покупателей только из кластеров ГДЕ ЕСТЬ НАШИ СКЛАДЫ
            # Кластеры без складов отображаем информационно, но qty не распределяем
            buyers_with_wh = sum(buyers for cluster, buyers in sorted_clusters
                                 if wh_by_cluster.get(cluster))
            buyers_with_wh = buyers_with_wh or total_buyers  # fallback

            remaining_target = needed_total
            clusters_with_wh = [(c, b) for c, b in sorted_clusters if wh_by_cluster.get(c)]
            clusters_no_wh   = [(c, b) for c, b in sorted_clusters if not wh_by_cluster.get(c)]

            for i, (cluster, buyers) in enumerate(sorted_clusters):
                buyer_pct  = round(buyers / total_buyers * 100, 1)
                our_whs    = wh_by_cluster.get(cluster, [])
                has_our_wh = len(our_whs) > 0

                if has_our_wh:
                    local_buyers += buyers

                if not has_our_wh:
                    # Нет нашего склада — показываем информационно, qty = 0
                    distribution.append({
                        "cluster":       cluster,
                        "buyers":        buyers,
                        "buyer_pct":     buyer_pct,
                        "has_warehouse": False,
                        "sub_whs":       [],
                        "qty_needed":    0,
                        "qty":           0,
                        "boxes":         0,
                    })
                    continue

                # Кол-во для кластера — ЦЕЛЕВОЙ остаток пропорционально покупателям
                # needed_total = сколько всего нужно товара на горизонт (уже посчитано выше)
                # Целевой остаток кластера = needed_total * доля_покупателей_кластера / all_buyers_with_wh
                is_last_with_wh = (cluster == clusters_with_wh[-1][0]) if clusters_with_wh else False
                if is_last_with_wh:
                    cluster_target = max(0, remaining_target)
                else:
                    raw = round(needed_total * buyers / buyers_with_wh)
                    cluster_target = max(0, raw)
                    remaining_target -= cluster_target

                # Темп продаж (spd) с каждого нашего склада — точнее чем кол-во заказов
                # spd учитывает когда товар появился на складе
                wh_demand = info.get("wh_demand", {})
                wh_sales = {}
                for w in our_whs:
                    wh_name = w["warehouse"]
                    wh_data = info.get("warehouses", {}).get(wh_name, {})
                    # Приоритет: wh_demand → spd склада → orders (fallback)
                    if wh_demand.get(wh_name, 0) > 0:
                        wh_sales[wh_name] = wh_demand[wh_name]
                    elif wh_data.get("spd", 0) > 0:
                        # Используем spd × 1000 для точности (чтобы не терять дроби при round)
                        wh_sales[wh_name] = round(wh_data["spd"] * 1000)
                    else:
                        wh_sales[wh_name] = wh_data.get("orders", 0)

                total_wh_sales = sum(wh_sales.values())

                # ВСЕ склады кластера активны
                # Склады без истории продаж получают минимальный вес = средний spd / 2
                active_whs = our_whs
                no_history = total_wh_sales == 0
                
                if total_wh_sales > 0:
                    # Есть продажи — складам без продаж даём половину среднего
                    whs_with_sales = [w for w in our_whs if wh_sales.get(w['warehouse'], 0) > 0]
                    avg_sales = total_wh_sales / len(whs_with_sales) if whs_with_sales else 1
                    min_weight = max(1, round(avg_sales * 0.5))
                    for w in our_whs:
                        if wh_sales.get(w['warehouse'], 0) == 0:
                            wh_sales[w['warehouse']] = min_weight
                else:
                    # Нет продаж ни на одном складе — делим поровну
                    for w in active_whs:
                        wh_sales[w['warehouse']] = 1

                total_wh_sales = sum(wh_sales.get(w["warehouse"], 0) for w in active_whs)
                if total_wh_sales == 0:
                    total_wh_sales = len(active_whs)

                # Коэф локализации склада
                def get_wh_loc_pct(wh_name, wh_orders_map, tot_orders):
                    wh_data   = wh_orders_map.get(wh_name, {})
                    wh_orders = wh_data.get("orders", 0)
                    if wh_orders == 0:
                        return 100.0
                    return wh_data.get("local_pct", 0)

                weighted_sales = {}
                for w in active_whs:
                    wn   = w["warehouse"]
                    loc  = get_wh_loc_pct(wn, all_wh_orders, total_all_orders)
                    coef = 0.5 + 0.5 * (loc / 100.0)
                    weighted_sales[wn] = wh_sales.get(wn, 1) * coef
                total_weighted = sum(weighted_sales.values()) or 1

                sub_whs = []
                sorted_wh = sorted(active_whs, key=lambda w: weighted_sales.get(w["warehouse"], 0), reverse=True)

                # Шаг 1: целевой остаток на каждом складе (пропорционально спросу)
                wh_targets = {}
                remaining_target = cluster_target
                for j, w in enumerate(sorted_wh):
                    wh_name = w["warehouse"]
                    w_sales = weighted_sales.get(wh_name, 0)
                    if j == len(sorted_wh) - 1:
                        wh_targets[wh_name] = max(0, remaining_target)
                    else:
                        raw = round(cluster_target * w_sales / total_weighted)
                        wh_targets[wh_name] = max(0, raw)
                        remaining_target -= wh_targets[wh_name]

                # Шаг 2: дефицит = целевой - текущий остаток на складе
                for j, w in enumerate(sorted_wh):
                    wh_name    = w["warehouse"]
                    sales      = wh_sales.get(wh_name, 0)
                    sales_pct  = round(sales / total_wh_sales * 100, 1)
                    wh_loc_pct = get_wh_loc_pct(wh_name, all_wh_orders, total_all_orders)

                    wh_stock   = info.get("warehouses", {}).get(wh_name, {}).get("stock", 0)
                    target     = wh_targets.get(wh_name, 0)
                    deficit    = max(0, target - wh_stock)
                    qty_to_ship = deficit

                    ktr, krp = get_ktr_krp(95.0)
                    log_cost = calc_logistics_cost(
                        volume_l, w["logistics_first"], w["logistics_extra"],
                        w["wb_logistics_coef"], ktr, price, krp, buyout_rate
                    )

                    wh_local_pct = info.get("warehouses", {}).get(wh_name, {}).get("local_pct", 0)

                    sub_whs.append({
                        "warehouse":         wh_name,
                        "sales":             sales,
                        "sales_pct":         sales_pct,
                        "wh_loc_pct":        wh_loc_pct,
                        "no_sales_history":  no_history,
                        "wh_stock":          wh_stock,
                        "qty_needed":        target,
                        "qty":               qty_to_ship,
                        "boxes":             math.ceil(qty_to_ship / box_qty) if qty_to_ship > 0 else 0,
                        "logistics_cost":    round(log_cost, 2),
                        "wh_localization":   round(wh_local_pct, 1),
                    })

                distribution.append({
                    "cluster":          cluster,
                    "buyers":           buyers,
                    "buyer_pct":        buyer_pct,
                    "has_warehouse":    True,
                    "no_history":       no_history,
                    "sub_whs":          sub_whs,
                    "qty_needed":       cluster_target,
                    "qty":              sum(s["qty"] for s in sub_whs),
                    "boxes":            sum(s["boxes"] for s in sub_whs),
                })

            # Доля локализации артикула — берём напрямую из данных заказов
            # Считается в process_data по ВСЕМ складам включая чужие
            our_orders       = sum(d.get("orders", 0) for wh, d in all_wh_orders.items()
                                   if wh in WAREHOUSE_REGION)

            # ── Пересчитываем to_ship по реальным дефицитам складов ──
            actual_to_ship = sum(
                s["qty"] for cluster_d in distribution
                for s in cluster_d.get("sub_whs", [])
            )
            # Ограничиваем: не отгружать больше чем реально нужно
            max_needed = max(0, needed_total - stock)
            if actual_to_ship > max_needed:
                # Пропорционально уменьшаем отгрузку по каждому складу
                if actual_to_ship > 0:
                    ratio = max_needed / actual_to_ship
                    for cluster_d in distribution:
                        for s in cluster_d.get("sub_whs", []):
                            s["qty"] = round(s["qty"] * ratio)
                            s["boxes"] = math.ceil(s["qty"] / box_qty) if s["qty"] > 0 else 0
                        cluster_d["qty"] = sum(s["qty"] for s in cluster_d.get("sub_whs", []))
                        cluster_d["boxes"] = sum(s["boxes"] for s in cluster_d.get("sub_whs", []))
                    actual_to_ship = sum(
                        s["qty"] for cluster_d in distribution
                        for s in cluster_d.get("sub_whs", [])
                    )
            # to_order = реальная отгрузка - то что возьмём с ФФ
            actual_to_order = max(0, actual_to_ship - ff_to_ship)
            if actual_to_order > 0 and box_qty > 1:
                actual_to_order = math.ceil(actual_to_order / box_qty) * box_qty
            to_ship = actual_to_ship
            to_order = actual_to_order
            localization_pct = info.get("localization_pct")
            if localization_pct is None:
                # Fallback: считаем из local_pct складов
                localization_pct = round(sum(d.get("local_pct", 0) for d in all_wh_orders.values()), 1)
            ktr, krp         = get_ktr_krp(localization_pct)

            # ── Прогнозная логистика по плану отгрузок ──
            # Средневзвешенная по складам из distribution (qty как вес)
            # С прогнозным КТР (если отгрузим правильно → локализация вырастет → КТР снизится)
            forecast_total_qty = 0
            forecast_weighted  = 0
            # Прогнозная локализация: 
            # Если раскидаем товар по складам пропорционально покупателям,
            # локализация ≈ доле покупателей из регионов с нашими складами
            # Берём максимум из текущей и потенциальной
            potential_loc = round(local_buyers / total_buyers * 100, 1) if total_buyers > 0 else localization_pct
            local_now    = info.get("local_orders_cnt", 0)
            total_now    = total_all_orders
            blend_loc    = round((local_now + to_ship) / (total_now + to_ship) * 100, 1) if (total_now + to_ship) > 0 else localization_pct
            forecast_loc = max(blend_loc, potential_loc)  # оптимистичный прогноз
            forecast_loc = min(forecast_loc, 100)
            forecast_ktr_val, forecast_krp_val = get_ktr_krp(forecast_loc)

            # Прогнозная логистика: базовые тарифы складов без КТР и ИРП
            # При локализации >60% КТР=1, ИРП=0 — это наша цель
            for cluster_d in distribution:
                for sw in cluster_d.get("sub_whs", []):
                    qty_sw = sw.get("qty", 0)
                    if qty_sw <= 0:
                        continue
                    wh_n = sw.get("warehouse", "")
                    wh_p = wh_par.get(wh_n, {})
                    if not wh_p:
                        wh_p = get_default_wh_params().get(wh_n, {})
                    if wh_p:
                        # Базовый тариф склада × КТР=1 + ИРП=0, с обратной логистикой
                        f_cost = calc_logistics_cost(
                            volume_l,
                            wh_p.get("logistics_first", 46),
                            wh_p.get("logistics_extra", 14),
                            wh_p.get("wb_logistics_coef", 1.0),
                            1.0, 0, 0.0, buyout_rate  # КТР=1, цена=0 (ИРП не важен), ИРП=0
                        )
                        forecast_weighted += f_cost * qty_sw
                        forecast_total_qty += qty_sw

            forecast_logistics = round(forecast_weighted / forecast_total_qty, 1) if forecast_total_qty > 0 else 0
            avg_logistics      = info.get("avg_logistics", 0)

            recommendations.append({
                "sku":              sku,
                "name":             info["name"],
                "nmid":             info.get("nmid", ""),
                "spd":              round(spd, 2),
                "volume_l":         round(volume_l, 2),
                "buyout_rate":      buyout_rate,
                "total_buyers":     total_buyers,
                "local_buyers":     local_buyers,
                "total_orders":     total_all_orders,
                "our_orders":       our_orders,
                "local_orders":     info.get("local_orders_cnt", 0),
                "localization_pct": localization_pct,
                "ktr":              ktr,
                "krp":              krp,
                "to_order":         to_order,
                "to_ship":          to_ship,
                "ff_stock":         ff_stock,
                "box_qty":          box_qty,
                "avg_logistics":    avg_logistics,
                "forecast_logistics": forecast_logistics,
                "forecast_loc_pct": forecast_loc,
                "forecast_ktr":     forecast_ktr_val,
                "distribution":     distribution,
            })

        recommendations.sort(key=lambda r: r["localization_pct"])

        return jsonify({
            "ok": True,
            "recommendations": recommendations,
        })

    except Exception as e:
        import traceback
        print("recommend_warehouses error:", traceback.format_exc())
        return jsonify({"ok": False, "error": str(e)})








@app.route("/api/debug_dims")
def debug_dims():
    token = get_store().get("token")
    if not token:
        return jsonify({"ok": False, "error": "Нет токена"})
    dims = fetch_dimensions(token)
    data = get_store().get("data", {})
    skus = list(data.get("skus", {}).keys())[:5]
    sample_dims = {k: v for k, v in list(dims.items())[:10]}
    return jsonify({
        "ok": True,
        "dims_count": len(dims),
        "dims_sample_keys": list(dims.keys())[:10],
        "dims_sample": sample_dims,
        "skus_sample": skus
    })

@app.route("/api/check_stock/<nmid>")
def check_stock(nmid):
    """Сверяем остатки с WB API напрямую"""
    token = get_store().get("token")
    if not token:
        return jsonify({"ok": False, "error": "Нет токена"})
    
    from datetime import datetime, timedelta
    date_from = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
    data = safe_get(f"{BASE}/supplier/stocks", token, {"dateFrom": date_from})
    
    if isinstance(data, dict) and "error" in data:
        return jsonify({"ok": False, "error": data["error"]})
    
    items = [x for x in (data or []) if str(x.get("nmId","")) == str(nmid)]
    total = sum(x.get("quantityFull", 0) for x in items)
    
    by_wh = {}
    for x in items:
        wh = x.get("warehouseName", "Неизвестно")
        by_wh[wh] = by_wh.get(wh, 0) + x.get("quantityFull", 0)
    
    return jsonify({
        "ok": True,
        "nmid": nmid,
        "total": total,
        "warehouses": sorted([{"wh": k, "qty": v} for k,v in by_wh.items() if v > 0], key=lambda x: -x["qty"])
    })


port = int(os.environ.get("PORT", 5000))

if __name__ == "__main__":
    print(f"\n  StockPilot запущен!")
    print(f"  Откройте браузер: http://localhost:{port}")
    print("=" * 50)
    app.run(debug=False, port=port, host="0.0.0.0")
