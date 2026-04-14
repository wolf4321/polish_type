"""
Dashboard Boilerplate — EKOMAT Analytics Hub.
FastAPI + asyncpg + Jinja2 + WooCommerce + PrestaShop.

Сайты:
  - dobraszklarnia.pl  (WooCommerce)  — webhook + API sync
  - oteko.pl           (PrestaShop)   — API pull каждые 15 мин
  - cieplarnia.pl      (PrestaShop)   — API pull каждые 15 мин

Railway deploy: добавь DB_CONNECT + API-ключи в env vars.
"""

import os
import io
import csv
import json
import asyncio
import hashlib
import hmac
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from base64 import b64encode

import asyncpg
import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from auth import auth_router, get_current_user


# ============================================================
# Конфигурация
# ============================================================
TZ = ZoneInfo("Europe/Warsaw")
DSN = lambda: os.getenv("DB_CONNECT")

# WooCommerce — dobraszklarnia.pl
WOO_URL     = os.getenv("WOO_URL", "https://dobraszklarnia.pl")
WOO_KEY     = os.getenv("WOO_KEY", "")       # Consumer Key
WOO_SECRET  = os.getenv("WOO_SECRET", "")     # Consumer Secret
WOO_WEBHOOK_SECRET = os.getenv("WOO_WEBHOOK_SECRET", "")  # Webhook secret для верификации

# PrestaShop — oteko.pl
PRESTA_OTEKO_URL = os.getenv("PRESTA_OTEKO_URL", "https://oteko.pl")
PRESTA_OTEKO_KEY = os.getenv("PRESTA_OTEKO_KEY", "")

# PrestaShop — cieplarnia.pl
PRESTA_CIEP_URL  = os.getenv("PRESTA_CIEP_URL", "https://cieplarnia.pl")
PRESTA_CIEP_KEY  = os.getenv("PRESTA_CIEP_KEY", "")

# Bitrix24 (фаза 3 — пока закомментировано)
# BITRIX_WEBHOOK_URL = os.getenv("BITRIX_WEBHOOK_URL")


# ============================================================
# Database connection pool
# ============================================================
_pool = None

async def get_db_pool():
    global _pool
    if _pool is None or _pool._closed:
        _pool = await asyncpg.create_pool(
            dsn=DSN(),
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
    return _pool


# ============================================================
# Lifespan — создание таблиц + запуск планировщика
# ============================================================
scheduler = AsyncIOScheduler(timezone=TZ)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # Таблица лидов (из форм, звонков)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id            SERIAL PRIMARY KEY,
                name          VARCHAR(200),
                phone         VARCHAR(50),
                email         VARCHAR(200),
                site          VARCHAR(200),
                brand_slug    VARCHAR(100) DEFAULT 'unknown',
                form_type     VARCHAR(50)  DEFAULT 'form',
                utm_source    VARCHAR(200),
                utm_medium    VARCHAR(200),
                utm_campaign  VARCHAR(500),
                utm_term      VARCHAR(500),
                gclid         VARCHAR(500),
                landing_page  TEXT,
                campaign_id   VARCHAR(100),
                ad_id         VARCHAR(100),
                status        VARCHAR(50) DEFAULT 'new',
                device        VARCHAR(50),
                created_at    TIMESTAMP DEFAULT NOW(),
                updated_at    TIMESTAMP DEFAULT NOW()
            )
        """)

        # Таблица заказов — единая для всех магазинов
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id              SERIAL PRIMARY KEY,
                external_id     VARCHAR(100),
                source          VARCHAR(50) NOT NULL,
                status          VARCHAR(50) DEFAULT 'new',
                customer_name   VARCHAR(300),
                customer_email  VARCHAR(200),
                customer_phone  VARCHAR(50),
                customer_city   VARCHAR(200),
                customer_zip    VARCHAR(20),
                customer_address TEXT,
                total           NUMERIC(12,2) DEFAULT 0,
                currency        VARCHAR(10) DEFAULT 'PLN',
                items_count     INTEGER DEFAULT 0,
                items_json      TEXT,
                payment_method  VARCHAR(100),
                shipping_method VARCHAR(200),
                note            TEXT,
                external_created TIMESTAMP,
                created_at      TIMESTAMP DEFAULT NOW(),
                updated_at      TIMESTAMP DEFAULT NOW(),
                UNIQUE(source, external_id)
            )
        """)

        # Таблица клиентов — уникальные по email/phone
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id              SERIAL PRIMARY KEY,
                name            VARCHAR(300),
                email           VARCHAR(200),
                phone           VARCHAR(50),
                city            VARCHAR(200),
                source          VARCHAR(50),
                orders_count    INTEGER DEFAULT 0,
                total_spent     NUMERIC(12,2) DEFAULT 0,
                first_order     TIMESTAMP,
                last_order      TIMESTAMP,
                created_at      TIMESTAMP DEFAULT NOW(),
                updated_at      TIMESTAMP DEFAULT NOW()
            )
        """)

        # Лог синхронизации — отслеживание процесса
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_log (
                id          SERIAL PRIMARY KEY,
                source      VARCHAR(50),
                status      VARCHAR(20),
                orders_new  INTEGER DEFAULT 0,
                orders_upd  INTEGER DEFAULT 0,
                error       TEXT,
                started_at  TIMESTAMP DEFAULT NOW(),
                finished_at TIMESTAMP
            )
        """)

    print("[startup] DB connected, all tables ready")

    # Фоновые задачи — синк магазинов
    scheduler.add_job(sync_woocommerce, CronTrigger(minute="*/15"), id="sync_woo", replace_existing=True)
    scheduler.add_job(sync_presta_oteko, CronTrigger(minute="*/15"), id="sync_oteko", replace_existing=True)
    scheduler.add_job(sync_presta_ciep, CronTrigger(minute="*/15"), id="sync_ciep", replace_existing=True)
    scheduler.start()
    print("[startup] scheduler started — syncing every 15 min")

    yield

    # --- Shutdown ---
    scheduler.shutdown(wait=False)
    if _pool and not _pool._closed:
        await _pool.close()
    print("[shutdown] cleanup done")


# ============================================================
# FastAPI app
# ============================================================
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
app.include_router(auth_router)


# ============================================================
# === ИНТЕГРАЦИИ: WooCommerce (dobraszklarnia.pl) ===
# ============================================================

def _woo_auth_header():
    """Basic Auth для WooCommerce REST API."""
    creds = b64encode(f"{WOO_KEY}:{WOO_SECRET}".encode()).decode()
    return {"Authorization": f"Basic {creds}"}


async def _woo_fetch_orders(session: aiohttp.ClientSession, page=1, per_page=50, after=None):
    """Получить страницу заказов из WooCommerce API."""
    params = {"page": page, "per_page": per_page, "orderby": "date", "order": "desc"}
    if after:
        params["after"] = after  # ISO 8601 формат
    url = f"{WOO_URL}/wp-json/wc/v3/orders"
    async with session.get(url, headers=_woo_auth_header(), params=params, ssl=True) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise Exception(f"WooCommerce API error {resp.status}: {text[:300]}")
        return await resp.json()


async def _save_order(conn, source: str, order_data: dict):
    """Сохранить/обновить заказ в unified таблице orders."""
    ext_id = str(order_data["external_id"])

    existing = await conn.fetchval(
        "SELECT id FROM orders WHERE source=$1 AND external_id=$2",
        source, ext_id
    )

    if existing:
        await conn.execute("""
            UPDATE orders SET
                status=$1, customer_name=$2, customer_email=$3, customer_phone=$4,
                customer_city=$5, customer_zip=$6, customer_address=$7,
                total=$8, currency=$9, items_count=$10, items_json=$11,
                payment_method=$12, shipping_method=$13, note=$14,
                external_created=$15, updated_at=NOW()
            WHERE source=$16 AND external_id=$17
        """,
            order_data.get("status", ""),
            order_data.get("customer_name", ""),
            order_data.get("customer_email", ""),
            order_data.get("customer_phone", ""),
            order_data.get("customer_city", ""),
            order_data.get("customer_zip", ""),
            order_data.get("customer_address", ""),
            float(order_data.get("total", 0)),
            order_data.get("currency", "PLN"),
            int(order_data.get("items_count", 0)),
            order_data.get("items_json", "[]"),
            order_data.get("payment_method", ""),
            order_data.get("shipping_method", ""),
            order_data.get("note", ""),
            order_data.get("external_created"),
            source, ext_id
        )
        return "updated"
    else:
        await conn.execute("""
            INSERT INTO orders (
                external_id, source, status, customer_name, customer_email,
                customer_phone, customer_city, customer_zip, customer_address,
                total, currency, items_count, items_json,
                payment_method, shipping_method, note, external_created
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
        """,
            ext_id, source,
            order_data.get("status", ""),
            order_data.get("customer_name", ""),
            order_data.get("customer_email", ""),
            order_data.get("customer_phone", ""),
            order_data.get("customer_city", ""),
            order_data.get("customer_zip", ""),
            order_data.get("customer_address", ""),
            float(order_data.get("total", 0)),
            order_data.get("currency", "PLN"),
            int(order_data.get("items_count", 0)),
            order_data.get("items_json", "[]"),
            order_data.get("payment_method", ""),
            order_data.get("shipping_method", ""),
            order_data.get("note", ""),
            order_data.get("external_created"),
        )
        return "new"


async def _upsert_customer(conn, source: str, name: str, email: str, phone: str, city: str,
                            total: float, order_date):
    """Обновить/создать клиента. Матчим по email (приоритет) или phone."""
    match_field = None
    match_value = None
    if email:
        match_field = "email"
        match_value = email
    elif phone:
        match_field = "phone"
        match_value = phone
    else:
        return

    existing = await conn.fetchrow(
        f"SELECT id, orders_count, total_spent FROM customers WHERE {match_field}=$1",
        match_value
    )

    if existing:
        await conn.execute("""
            UPDATE customers SET
                name = COALESCE(NULLIF($1, ''), name),
                city = COALESCE(NULLIF($2, ''), city),
                orders_count = orders_count + 1,
                total_spent = total_spent + $3,
                last_order = $4,
                updated_at = NOW()
            WHERE id = $5
        """, name, city, total, order_date, existing["id"])
    else:
        await conn.execute("""
            INSERT INTO customers (name, email, phone, city, source, orders_count, total_spent, first_order, last_order)
            VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $7)
        """, name, email, phone, city, source, total, order_date)


def _parse_woo_order(woo: dict) -> dict:
    """Маппинг WooCommerce order → наш формат."""
    billing = woo.get("billing", {})
    items = []
    for item in woo.get("line_items", []):
        items.append({
            "name": item.get("name", ""),
            "qty": item.get("quantity", 1),
            "price": item.get("total", "0"),
            "sku": item.get("sku", ""),
        })

    ext_created = None
    if woo.get("date_created"):
        try:
            ext_created = datetime.fromisoformat(woo["date_created"].replace("Z", "+00:00"))
        except Exception:
            ext_created = datetime.now(TZ)

    return {
        "external_id": str(woo["id"]),
        "status": woo.get("status", "unknown"),
        "customer_name": f"{billing.get('first_name', '')} {billing.get('last_name', '')}".strip(),
        "customer_email": billing.get("email", ""),
        "customer_phone": billing.get("phone", ""),
        "customer_city": billing.get("city", ""),
        "customer_zip": billing.get("postcode", ""),
        "customer_address": f"{billing.get('address_1', '')} {billing.get('address_2', '')}".strip(),
        "total": float(woo.get("total", 0)),
        "currency": woo.get("currency", "PLN"),
        "items_count": len(items),
        "items_json": json.dumps(items, ensure_ascii=False),
        "payment_method": woo.get("payment_method_title", ""),
        "shipping_method": ", ".join(s.get("method_title", "") for s in woo.get("shipping_lines", [])),
        "note": woo.get("customer_note", ""),
        "external_created": ext_created,
    }


async def sync_woocommerce():
    """Фоновая задача: синк заказов из WooCommerce (dobraszklarnia.pl)."""
    if not WOO_KEY or not WOO_SECRET:
        return

    source = "dobraszklarnia"
    pool = await get_db_pool()

    # Определяем с какой даты синкать (последний заказ + запас)
    async with pool.acquire() as conn:
        last = await conn.fetchval(
            "SELECT MAX(external_created) FROM orders WHERE source=$1", source
        )
    after = None
    if last:
        after = (last - timedelta(hours=2)).isoformat()

    log_id = None
    async with pool.acquire() as conn:
        log_id = await conn.fetchval(
            "INSERT INTO sync_log (source, status) VALUES ($1, 'running') RETURNING id", source
        )

    new_count = 0
    upd_count = 0

    try:
        async with aiohttp.ClientSession() as session:
            page = 1
            while True:
                orders = await _woo_fetch_orders(session, page=page, per_page=50, after=after)
                if not orders:
                    break

                async with pool.acquire() as conn:
                    for woo_order in orders:
                        parsed = _parse_woo_order(woo_order)
                        result = await _save_order(conn, source, parsed)
                        if result == "new":
                            new_count += 1
                            # Upsert customer только для новых
                            await _upsert_customer(
                                conn, source,
                                parsed["customer_name"], parsed["customer_email"],
                                parsed["customer_phone"], parsed["customer_city"],
                                parsed["total"], parsed["external_created"]
                            )
                        else:
                            upd_count += 1

                if len(orders) < 50:
                    break
                page += 1

        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE sync_log SET status='ok', orders_new=$1, orders_upd=$2, finished_at=NOW() WHERE id=$3",
                new_count, upd_count, log_id
            )
        print(f"[sync-woo] done: +{new_count} new, ~{upd_count} updated")

    except Exception as e:
        print(f"[sync-woo] ERROR: {e}")
        traceback.print_exc()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE sync_log SET status='error', error=$1, finished_at=NOW() WHERE id=$2",
                str(e)[:500], log_id
            )


# ============================================================
# === ИНТЕГРАЦИИ: PrestaShop (oteko.pl + cieplarnia.pl) ===
# ============================================================

async def _presta_fetch_orders(session: aiohttp.ClientSession, base_url: str, api_key: str,
                                page=0, limit=50, date_after=None):
    """Получить заказы из PrestaShop Webservice API."""
    url = f"{base_url}/api/orders"
    params = {
        "output_format": "JSON",
        "display": "full",
        "limit": f"{page * limit},{limit}",
        "sort": "[date_add_DESC]",
    }
    if date_after:
        params["filter[date_add]"] = f"[{date_after},]"

    auth = aiohttp.BasicAuth(api_key, "")  # PrestaShop: key как логин, пустой пароль
    async with session.get(url, params=params, auth=auth, ssl=True) as resp:
        if resp.status == 401:
            raise Exception("PrestaShop API: неверный ключ или Webservice отключён")
        if resp.status != 200:
            text = await resp.text()
            raise Exception(f"PrestaShop API error {resp.status}: {text[:300]}")
        data = await resp.json()
        return data.get("orders", [])


async def _presta_fetch_customer(session: aiohttp.ClientSession, base_url: str, api_key: str,
                                  customer_id):
    """Получить данные клиента по ID."""
    if not customer_id:
        return {}
    url = f"{base_url}/api/customers/{customer_id}"
    params = {"output_format": "JSON"}
    auth = aiohttp.BasicAuth(api_key, "")
    try:
        async with session.get(url, params=params, auth=auth, ssl=True) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
            return data.get("customer", {})
    except Exception:
        return {}


async def _presta_fetch_address(session: aiohttp.ClientSession, base_url: str, api_key: str,
                                 address_id):
    """Получить адрес по ID."""
    if not address_id:
        return {}
    url = f"{base_url}/api/addresses/{address_id}"
    params = {"output_format": "JSON"}
    auth = aiohttp.BasicAuth(api_key, "")
    try:
        async with session.get(url, params=params, auth=auth, ssl=True) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
            return data.get("address", {})
    except Exception:
        return {}


# Маппинг статусов PrestaShop (стандартные ID → текст)
PRESTA_STATUS_MAP = {
    "1": "pending_payment",
    "2": "payment_accepted",
    "3": "processing",
    "4": "shipped",
    "5": "delivered",
    "6": "cancelled",
    "7": "refunded",
    "8": "payment_error",
    "9": "on_backorder",
    "10": "awaiting_bankwire",
    "11": "awaiting_cash",
    "12": "remote_payment",
}


def _parse_presta_order(order: dict, customer: dict, address: dict) -> dict:
    """Маппинг PrestaShop order → наш формат."""
    items = []
    order_rows = order.get("associations", {}).get("order_rows", [])
    if isinstance(order_rows, list):
        for row in order_rows:
            items.append({
                "name": row.get("product_name", ""),
                "qty": int(row.get("product_quantity", 1)),
                "price": row.get("product_price", "0"),
                "sku": row.get("product_reference", ""),
            })

    ext_created = None
    if order.get("date_add"):
        try:
            ext_created = datetime.fromisoformat(order["date_add"])
        except Exception:
            ext_created = datetime.now(TZ)

    status_id = str(order.get("current_state", ""))
    status = PRESTA_STATUS_MAP.get(status_id, f"state_{status_id}")

    cust_name = f"{customer.get('firstname', '')} {customer.get('lastname', '')}".strip()
    if not cust_name:
        cust_name = f"{address.get('firstname', '')} {address.get('lastname', '')}".strip()

    return {
        "external_id": str(order.get("id", "")),
        "status": status,
        "customer_name": cust_name,
        "customer_email": customer.get("email", ""),
        "customer_phone": address.get("phone", "") or address.get("phone_mobile", ""),
        "customer_city": address.get("city", ""),
        "customer_zip": address.get("postcode", ""),
        "customer_address": f"{address.get('address1', '')} {address.get('address2', '')}".strip(),
        "total": float(order.get("total_paid", 0)),
        "currency": order.get("id_currency", "PLN"),
        "items_count": len(items),
        "items_json": json.dumps(items, ensure_ascii=False),
        "payment_method": order.get("payment", ""),
        "shipping_method": "",
        "note": "",
        "external_created": ext_created,
    }


async def _sync_prestashop(source: str, base_url: str, api_key: str):
    """Универсальная функция синка PrestaShop магазина."""
    if not api_key:
        return

    pool = await get_db_pool()

    # Определяем с какой даты синкать
    async with pool.acquire() as conn:
        last = await conn.fetchval(
            "SELECT MAX(external_created) FROM orders WHERE source=$1", source
        )
    date_after = None
    if last:
        date_after = (last - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")

    log_id = None
    async with pool.acquire() as conn:
        log_id = await conn.fetchval(
            "INSERT INTO sync_log (source, status) VALUES ($1, 'running') RETURNING id", source
        )

    new_count = 0
    upd_count = 0

    try:
        async with aiohttp.ClientSession() as session:
            page = 0
            while True:
                orders = await _presta_fetch_orders(
                    session, base_url, api_key,
                    page=page, limit=50, date_after=date_after
                )
                if not orders:
                    break

                async with pool.acquire() as conn:
                    for ps_order in orders:
                        # Подтягиваем клиента и адрес
                        customer = await _presta_fetch_customer(
                            session, base_url, api_key, ps_order.get("id_customer")
                        )
                        address = await _presta_fetch_address(
                            session, base_url, api_key, ps_order.get("id_address_delivery")
                        )

                        parsed = _parse_presta_order(ps_order, customer, address)
                        result = await _save_order(conn, source, parsed)
                        if result == "new":
                            new_count += 1
                            await _upsert_customer(
                                conn, source,
                                parsed["customer_name"], parsed["customer_email"],
                                parsed["customer_phone"], parsed["customer_city"],
                                parsed["total"], parsed["external_created"]
                            )
                        else:
                            upd_count += 1

                if len(orders) < 50:
                    break
                page += 1

        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE sync_log SET status='ok', orders_new=$1, orders_upd=$2, finished_at=NOW() WHERE id=$3",
                new_count, upd_count, log_id
            )
        print(f"[sync-{source}] done: +{new_count} new, ~{upd_count} updated")

    except Exception as e:
        print(f"[sync-{source}] ERROR: {e}")
        traceback.print_exc()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE sync_log SET status='error', error=$1, finished_at=NOW() WHERE id=$2",
                str(e)[:500], log_id
            )


async def sync_presta_oteko():
    await _sync_prestashop("oteko", PRESTA_OTEKO_URL, PRESTA_OTEKO_KEY)

async def sync_presta_ciep():
    await _sync_prestashop("cieplarnia", PRESTA_CIEP_URL, PRESTA_CIEP_KEY)


# ============================================================
# === СТРАНИЦЫ ===
# ============================================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = await get_current_user(request)
    pool = await get_db_pool()

    async with pool.acquire() as conn:
        # Статистика лидов
        total_leads = await conn.fetchval("SELECT COUNT(*) FROM leads")
        today_leads = await conn.fetchval(
            "SELECT COUNT(*) FROM leads WHERE created_at::date = $1", date.today()
        )
        recent = await conn.fetch(
            "SELECT * FROM leads ORDER BY created_at DESC LIMIT 20"
        )

        # Статистика заказов
        total_orders = await conn.fetchval("SELECT COUNT(*) FROM orders")
        today_orders = await conn.fetchval(
            "SELECT COUNT(*) FROM orders WHERE created_at::date = $1", date.today()
        )
        total_revenue = await conn.fetchval("SELECT COALESCE(SUM(total), 0) FROM orders") or 0
        total_customers = await conn.fetchval("SELECT COUNT(*) FROM customers")

        # Заказы по источникам
        by_source = await conn.fetch("""
            SELECT source, COUNT(*) as cnt, COALESCE(SUM(total), 0) as revenue
            FROM orders GROUP BY source ORDER BY cnt DESC
        """)

        # Последний синк
        last_syncs = await conn.fetch("""
            SELECT DISTINCT ON (source) source, status, orders_new, orders_upd, finished_at
            FROM sync_log ORDER BY source, finished_at DESC NULLS LAST
        """)

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "user": user,
            "total_leads": total_leads,
            "today_leads": today_leads,
            "recent_leads": recent,
            "total_orders": total_orders,
            "today_orders": today_orders,
            "total_revenue": total_revenue,
            "total_customers": total_customers,
            "by_source": by_source,
            "last_syncs": last_syncs,
        },
    )


@app.get("/orders", response_class=HTMLResponse)
async def orders_page(request: Request):
    user = await get_current_user(request)
    return templates.TemplateResponse(
        request=request,
        name="orders.html",
        context={"user": user},
    )


# ============================================================
# === API: Статистика ===
# ============================================================

@app.get("/api/stats")
async def api_stats(
    date_from: str = Query(None),
    date_to:   str = Query(None),
):
    pool = await get_db_pool()
    d_from = date.fromisoformat(date_from) if date_from else date.today() - timedelta(days=30)
    d_to   = date.fromisoformat(date_to)   if date_to   else date.today()

    async with pool.acquire() as conn:
        leads = await conn.fetchrow("""
            SELECT
                COUNT(*)                                              AS total,
                COUNT(*) FILTER (WHERE form_type = 'call')            AS calls,
                COUNT(*) FILTER (WHERE form_type IN ('form','chat'))  AS forms,
                COUNT(*) FILTER (WHERE status = 'new')                AS new_leads,
                COUNT(*) FILTER (WHERE status = 'processed')          AS processed
            FROM leads
            WHERE created_at::date >= $1 AND created_at::date <= $2
        """, d_from, d_to)

        orders = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(total), 0) AS revenue,
                COUNT(DISTINCT customer_email) FILTER (WHERE customer_email != '') AS unique_customers,
                COUNT(*) FILTER (WHERE source = 'dobraszklarnia') AS woo_count,
                COUNT(*) FILTER (WHERE source = 'oteko') AS oteko_count,
                COUNT(*) FILTER (WHERE source = 'cieplarnia') AS ciep_count
            FROM orders
            WHERE created_at::date >= $1 AND created_at::date <= $2
        """, d_from, d_to)

    return JSONResponse({
        "leads": dict(leads),
        "orders": {k: float(v) if isinstance(v, (int, float)) else v for k, v in dict(orders).items()},
        "date_from": d_from.isoformat(),
        "date_to": d_to.isoformat(),
    })


# ============================================================
# === API: Заказы (для таблицы + экспорт) ===
# ============================================================

@app.get("/api/orders")
async def api_orders(
    limit:  int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    source: str = Query(None),
    status: str = Query(None),
    search: str = Query(None),
    date_from: str = Query(None),
    date_to:   str = Query(None),
):
    pool = await get_db_pool()
    conditions = ["1=1"]
    params = []

    if source:
        params.append(source)
        conditions.append(f"source = ${len(params)}")
    if status:
        params.append(status)
        conditions.append(f"status = ${len(params)}")
    if search:
        params.append(f"%{search}%")
        conditions.append(f"(customer_name ILIKE ${len(params)} OR customer_phone ILIKE ${len(params)} OR customer_email ILIKE ${len(params)})")
    if date_from:
        params.append(date.fromisoformat(date_from))
        conditions.append(f"created_at::date >= ${len(params)}")
    if date_to:
        params.append(date.fromisoformat(date_to))
        conditions.append(f"created_at::date <= ${len(params)}")

    where = " AND ".join(conditions)

    params.extend([limit, offset])
    q = f"""
        SELECT * FROM orders
        WHERE {where}
        ORDER BY COALESCE(external_created, created_at) DESC
        LIMIT ${len(params)-1} OFFSET ${len(params)}
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(q, *params)
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM orders WHERE {where}",
            *params[:-2]
        )

    def serialize_row(r):
        d = dict(r)
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
        return d

    return JSONResponse({
        "total": total,
        "orders": [serialize_row(r) for r in rows],
    })


# ============================================================
# === API: Экспорт заказов в CSV ===
# ============================================================

@app.get("/api/orders/export")
async def export_orders(
    source: str = Query(None),
    status: str = Query(None),
    date_from: str = Query(None),
    date_to:   str = Query(None),
    fmt: str = Query("csv"),  # csv или xlsx (xlsx добавим позже)
):
    pool = await get_db_pool()
    conditions = ["1=1"]
    params = []

    if source:
        params.append(source)
        conditions.append(f"source = ${len(params)}")
    if status:
        params.append(status)
        conditions.append(f"status = ${len(params)}")
    if date_from:
        params.append(date.fromisoformat(date_from))
        conditions.append(f"created_at::date >= ${len(params)}")
    if date_to:
        params.append(date.fromisoformat(date_to))
        conditions.append(f"created_at::date <= ${len(params)}")

    where = " AND ".join(conditions)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT * FROM orders WHERE {where} ORDER BY COALESCE(external_created, created_at) DESC",
            *params
        )

    # CSV export
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow([
        "ID", "Источник", "Внешний ID", "Статус", "Клиент", "Email", "Телефон",
        "Город", "Адрес", "Сумма", "Валюта", "Товаров", "Оплата", "Доставка",
        "Дата заказа", "Дата синка"
    ])
    for r in rows:
        writer.writerow([
            r["id"], r["source"], r["external_id"], r["status"],
            r["customer_name"], r["customer_email"], r["customer_phone"],
            r["customer_city"], r["customer_address"],
            r["total"], r["currency"], r["items_count"],
            r["payment_method"], r["shipping_method"],
            r["external_created"].strftime("%Y-%m-%d %H:%M") if r["external_created"] else "",
            r["created_at"].strftime("%Y-%m-%d %H:%M") if r["created_at"] else "",
        ])

    filename = f"orders_{date.today().isoformat()}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# ============================================================
# === API: Клиенты ===
# ============================================================

@app.get("/api/customers")
async def api_customers(
    limit:  int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    search: str = Query(None),
):
    pool = await get_db_pool()
    conditions = ["1=1"]
    params = []

    if search:
        params.append(f"%{search}%")
        conditions.append(f"(name ILIKE ${len(params)} OR phone ILIKE ${len(params)} OR email ILIKE ${len(params)})")

    where = " AND ".join(conditions)
    params.extend([limit, offset])

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT * FROM customers WHERE {where} ORDER BY last_order DESC NULLS LAST LIMIT ${len(params)-1} OFFSET ${len(params)}",
            *params
        )
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM customers WHERE {where}",
            *params[:-2]
        )

    def serialize(r):
        d = dict(r)
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
        return d

    return JSONResponse({"total": total, "customers": [serialize(r) for r in rows]})


# ============================================================
# === API: Лиды (оставляем из оригинала) ===
# ============================================================

@app.get("/api/leads")
async def api_leads(
    limit:  int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    status: str = Query(None),
):
    pool = await get_db_pool()
    conditions = ["1=1"]
    params = []

    if status:
        params.append(status)
        conditions.append(f"status = ${len(params)}")

    params.extend([limit, offset])
    q = f"""
        SELECT * FROM leads
        WHERE {' AND '.join(conditions)}
        ORDER BY created_at DESC
        LIMIT ${len(params)-1} OFFSET ${len(params)}
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(q, *params)
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM leads WHERE {' AND '.join(conditions)}",
            *params[:-2]
        )

    return JSONResponse({
        "total": total,
        "leads": [dict(r) | {"created_at": r["created_at"].isoformat()} for r in rows],
    })


# ============================================================
# === Webhooks ===
# ============================================================

@app.api_route("/api/webhook/lead", methods=["GET", "POST"])
async def webhook_lead(request: Request):
    data = {}
    data.update(dict(request.query_params))
    raw = await request.body()
    if raw:
        ct = request.headers.get("content-type", "")
        if "json" in ct:
            data.update(json.loads(raw))
        elif "form" in ct or "urlencoded" in ct:
            from urllib.parse import parse_qs
            for k, v in parse_qs(raw.decode()).items():
                data[k] = v[0] if len(v) == 1 else v

    name  = data.get("name", "")
    phone = data.get("phone", "")
    email = data.get("email", "")

    utm_source   = data.get("utm_source", "")
    utm_medium   = data.get("utm_medium", "")
    utm_campaign = data.get("utm_campaign", "")
    utm_term     = data.get("utm_term", "")
    gclid        = data.get("gclid", "")
    landing      = data.get("landing_page") or data.get("referer") or ""

    if not phone and not email:
        return JSONResponse({"ok": False, "error": "phone or email required"}, status_code=400)

    pool = await get_db_pool()
    async with pool.acquire() as conn:
        lead_id = await conn.fetchval("""
            INSERT INTO leads (name, phone, email, utm_source, utm_medium,
                               utm_campaign, utm_term, gclid, landing_page)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            RETURNING id
        """, name, phone, email, utm_source, utm_medium,
             utm_campaign, utm_term, gclid, landing)

    print(f"[webhook] new lead #{lead_id}: {phone or email}")
    return JSONResponse({"ok": True, "lead_id": lead_id})


@app.api_route("/api/webhook/call", methods=["GET", "POST"])
async def webhook_call(request: Request):
    raw = await request.body()
    data = dict(request.query_params)
    if raw:
        ct = request.headers.get("content-type", "")
        if "json" in ct:
            data.update(json.loads(raw))
        elif "form" in ct or "urlencoded" in ct:
            from urllib.parse import parse_qs
            for k, v in parse_qs(raw.decode()).items():
                data[k] = v[0]

    phone = data.get("phone", "")
    lead_id = None
    if phone:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            lead_id = await conn.fetchval(
                "SELECT id FROM leads WHERE phone = $1 ORDER BY created_at DESC LIMIT 1", phone
            )
            if lead_id:
                await conn.execute("""
                    UPDATE leads SET form_type = 'call', device = 'phone', updated_at = NOW()
                    WHERE id = $1
                """, lead_id)

    return JSONResponse({"ok": True, "matched_lead": lead_id})


@app.api_route("/api/webhook/woo", methods=["POST"])
async def webhook_woo(request: Request):
    """Webhook от WooCommerce — мгновенный приём нового заказа."""
    raw = await request.body()
    if not raw:
        return JSONResponse({"ok": False, "error": "empty body"}, status_code=400)

    # Верификация подписи (если задан WOO_WEBHOOK_SECRET)
    if WOO_WEBHOOK_SECRET:
        signature = request.headers.get("X-WC-Webhook-Signature", "")
        expected = b64encode(
            hmac.new(WOO_WEBHOOK_SECRET.encode(), raw, hashlib.sha256).digest()
        ).decode()
        if not hmac.compare_digest(signature, expected):
            print("[webhook-woo] invalid signature!")
            return JSONResponse({"ok": False, "error": "invalid signature"}, status_code=401)

    data = json.loads(raw)

    # WooCommerce шлёт ping при создании webhook — игнорируем
    if not data.get("id"):
        return JSONResponse({"ok": True, "note": "ping acknowledged"})

    source = "dobraszklarnia"
    parsed = _parse_woo_order(data)

    pool = await get_db_pool()
    async with pool.acquire() as conn:
        result = await _save_order(conn, source, parsed)
        if result == "new":
            await _upsert_customer(
                conn, source,
                parsed["customer_name"], parsed["customer_email"],
                parsed["customer_phone"], parsed["customer_city"],
                parsed["total"], parsed["external_created"]
            )

    print(f"[webhook-woo] order #{parsed['external_id']} — {result}")
    return JSONResponse({"ok": True, "order_id": parsed["external_id"], "result": result})


# ============================================================
# === Ручной синк + лог ===
# ============================================================

@app.get("/admin/sync")
async def manual_sync(source: str = Query(None)):
    """Ручной запуск синхронизации. /admin/sync?source=dobraszklarnia"""
    if source == "dobraszklarnia":
        asyncio.create_task(sync_woocommerce())
        return JSONResponse({"ok": True, "message": "WooCommerce sync started"})
    elif source == "oteko":
        asyncio.create_task(sync_presta_oteko())
        return JSONResponse({"ok": True, "message": "Oteko sync started"})
    elif source == "cieplarnia":
        asyncio.create_task(sync_presta_ciep())
        return JSONResponse({"ok": True, "message": "Cieplarnia sync started"})
    else:
        # Синк всех
        asyncio.create_task(sync_woocommerce())
        asyncio.create_task(sync_presta_oteko())
        asyncio.create_task(sync_presta_ciep())
        return JSONResponse({"ok": True, "message": "All syncs started"})


@app.get("/api/sync-log")
async def api_sync_log(limit: int = Query(20)):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM sync_log ORDER BY started_at DESC LIMIT $1", limit
        )

    def serialize(r):
        d = dict(r)
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
        return d

    return JSONResponse({"log": [serialize(r) for r in rows]})


# ============================================================
# === Миграции / Healthcheck ===
# ============================================================

@app.get("/admin/migrate")
async def run_migration():
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        migrations = [
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS status VARCHAR(50) DEFAULT 'new'",
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS campaign_id VARCHAR(100)",
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS ad_id VARCHAR(100)",
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS shipping_method VARCHAR(200)",
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS customer_zip VARCHAR(20)",
            "ALTER TABLE customers ADD COLUMN IF NOT EXISTS source VARCHAR(50)",
            # Добавляй свои миграции:
        ]
        results = []
        for m in migrations:
            try:
                await conn.execute(m)
                results.append(f"OK: {m[:80]}")
            except Exception as e:
                results.append(f"ERR: {m[:80]} — {e}")

    return JSONResponse({"migrations": results})


@app.get("/health")
async def health():
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return JSONResponse({"status": "ok", "db": "connected"})
    except Exception as e:
        return JSONResponse({"status": "error", "db": str(e)}, status_code=500)
