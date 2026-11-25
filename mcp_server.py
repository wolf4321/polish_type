# -*- coding: utf-8 -*-
"""
mcp_server.py — удалённый MCP-сервер EKOMAT Dashboard.

Позволяет Claude (и любому другому MCP-клиенту) читать и править заказы
прямо в этой базе: искать, менять комментарий, статус, приоритет, отметку
экспорта, создавать и удалять ручные заказы, смотреть статистику и синк.

Транспорт: Streamable HTTP (JSON-RPC 2.0 поверх обычного POST).
Адрес:     https://<домен>/mcp/<MCP_TOKEN>
           либо https://<домен>/mcp  с заголовком Authorization: Bearer <MCP_TOKEN>

Переменные окружения:
    MCP_TOKEN     — секрет. Без него эндпоинт выключен (503).
    MCP_READONLY  — "true" → разрешены только read-only инструменты.

Модуль намеренно ничего не импортирует из main.py (чтобы не было кольцевого
импорта). main.py при старте вызывает configure_mcp(...) и передаёт зависимости.
"""

import os
import json
import uuid
import traceback
from datetime import datetime, date
from decimal import Decimal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

mcp_router = APIRouter()

PROTOCOL_VERSION = "2025-06-18"
SUPPORTED_PROTOCOLS = {"2025-06-18", "2025-03-26", "2024-11-05"}

# ── Зависимости, которые прокидывает main.py ──────────────────────────────
_DEPS = {
    "get_db_pool": None,   # async () -> asyncpg pool
    "sync_map": {},        # {"dobraszklarnia": coro_fn, ...}
    "tz": None,
    "instance_id": "v2",
    "sync_enabled": False,
}


def configure_mcp(**kwargs):
    """Вызывается из main.py один раз при импорте."""
    _DEPS.update({k: v for k, v in kwargs.items() if v is not None})


def _token() -> str:
    return (os.getenv("MCP_TOKEN") or "").strip()


def _readonly() -> bool:
    return (os.getenv("MCP_READONLY") or "").strip().lower() in ("1", "true", "yes", "on")


async def _pool():
    fn = _DEPS.get("get_db_pool")
    if fn is None:
        raise RuntimeError("MCP не сконфигурирован: нет get_db_pool")
    return await fn()


# ── Сериализация строк БД ─────────────────────────────────────────────────
def _val(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


ORDER_FIELDS = [
    "id", "external_id", "source", "seller_account", "status",
    "customer_name", "customer_email", "customer_phone",
    "customer_city", "customer_zip", "customer_address",
    "total", "shipping_cost", "currency",
    "payment_method", "shipping_method",
    "product_type", "configuration", "invoice_nip",
    "customer_comment", "note",
    "exported", "exported_at", "is_priority",
    "created_by_email", "external_created", "created_at",
]


def _order_row(row, with_items=False):
    d = {}
    for f in ORDER_FIELDS:
        if f in row:
            d[f] = _val(row[f])
    if with_items:
        try:
            d["items"] = json.loads(row["items_json"] or "[]")
        except Exception:
            d["items"] = []
    else:
        try:
            items = json.loads(row["items_json"] or "[]")
            d["products"] = ", ".join(
                "%sx %s" % (it.get("qty", 1), it.get("full_name") or it.get("name") or it.get("sku") or "?")
                for it in items[:3]
            )
        except Exception:
            d["products"] = ""
    return d


# ══════════════════════════════════════════════════════════════════════════
#  ОПИСАНИЕ ИНСТРУМЕНТОВ
# ══════════════════════════════════════════════════════════════════════════
def _str(desc, **extra):
    d = {"type": "string", "description": desc}
    d.update(extra)
    return d


def _int(desc, **extra):
    d = {"type": "integer", "description": desc}
    d.update(extra)
    return d


def _bool(desc):
    return {"type": "boolean", "description": desc}


def _num(desc):
    return {"type": "number", "description": desc}


TOOLS = [
    {
        "name": "list_orders",
        "title": "Найти заказы",
        "description": (
            "Поиск и фильтрация заказов. Свободный поиск идёт по имени клиента, "
            "телефону, e-mail, номеру заказа, городу, адресу, комментарию, названию "
            "товара, NIP и аккаунту продавца. Возвращает краткие карточки заказов."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": _str("Свободный поиск. Несколько слов = И (все должны найтись)."),
                "source": _str("Магазин: dobraszklarnia, oteko, cieplarnia, allegro, erli, manual. Можно через запятую."),
                "seller_account": _str("Аккаунт Allegro, например drogatrade."),
                "status": _str("Статус заказа, например processing, completed, cancelled."),
                "date_from": _str("Дата с, формат YYYY-MM-DD."),
                "date_to": _str("Дата по, формат YYYY-MM-DD."),
                "exported": _str("yes — только выгруженные, no — только невыгруженные.", enum=["yes", "no"]),
                "priority": _str("yes — только приоритетные (со звёздочкой).", enum=["yes"]),
                "limit": _int("Сколько вернуть, по умолчанию 20, максимум 100."),
                "offset": _int("Сместить выборку (для листания)."),
            },
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "get_order",
        "title": "Открыть заказ",
        "description": "Полная карточка одного заказа со всеми товарами. Нужен id или external_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_id": _int("Внутренний id заказа."),
                "external_id": _str("Номер заказа в магазине, например 1234567890."),
            },
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "stats",
        "title": "Статистика",
        "description": "Сводка за период: количество заказов, выручка, разбивка по магазинам и статусам.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "date_from": _str("Дата с, YYYY-MM-DD. По умолчанию — начало текущего месяца."),
                "date_to": _str("Дата по, YYYY-MM-DD. По умолчанию — сегодня."),
                "source": _str("Ограничить одним магазином."),
            },
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "sync_status",
        "title": "Состояние синхронизации",
        "description": "Последние записи журнала синхронизации: что, когда, сколько новых заказов, были ли ошибки.",
        "inputSchema": {
            "type": "object",
            "properties": {"limit": _int("Сколько записей, по умолчанию 10.")},
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "set_comment",
        "title": "Изменить комментарий",
        "description": "Записать комментарий менеджера в заказ (тот, что виден красным в таблице).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_id": _int("Внутренний id заказа."),
                "comment": _str("Новый текст комментария. Пустая строка очищает."),
            },
            "required": ["order_id", "comment"],
            "additionalProperties": False,
        },
    },
    {
        "name": "set_status",
        "title": "Изменить статус",
        "description": (
            "Поменять статус заказа в панели. ВНИМАНИЕ: меняется только у нас, "
            "обратно в магазин (WooCommerce/PrestaShop/Allegro) не уходит, и полная "
            "синхронизация может перезаписать значение обратно."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_id": _int("Внутренний id заказа."),
                "status": _str("Новый статус, например processing, completed, cancelled."),
            },
            "required": ["order_id", "status"],
            "additionalProperties": False,
        },
    },
    {
        "name": "set_priority",
        "title": "Звёздочка",
        "description": "Поставить или снять приоритет (звёздочку) у заказа.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_id": _int("Внутренний id заказа."),
                "is_priority": _bool("true — поставить звезду, false — снять."),
            },
            "required": ["order_id", "is_priority"],
            "additionalProperties": False,
        },
    },
    {
        "name": "set_exported",
        "title": "Отметка выгрузки",
        "description": "Отметить заказ как выгруженный в доставку или снять отметку.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_id": _int("Внутренний id заказа."),
                "exported": _bool("true — отмечен как выгруженный."),
            },
            "required": ["order_id", "exported"],
            "additionalProperties": False,
        },
    },
    {
        "name": "create_order",
        "title": "Создать ручной заказ",
        "description": "Добавить заказ вручную. Такой заказ не перезаписывается синхронизацией.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "customer_name": _str("Имя и фамилия клиента."),
                "customer_phone": _str("Телефон."),
                "customer_email": _str("E-mail."),
                "customer_city": _str("Город."),
                "customer_address": _str("Улица, дом, квартира."),
                "product_name": _str("Название товара."),
                "price": _num("Цена товара, PLN."),
                "shipping_cost": _num("Доставка, PLN."),
                "payment_method": _str("Способ оплаты, например Przelew."),
                "product_type": _str("Тип товара."),
                "configuration": _str("Комплектация."),
                "comment": _str("Комментарий."),
                "status": _str("Статус, по умолчанию processing."),
                "source": _str("Метка источника, по умолчанию manual."),
                "seller_account": _str("Аккаунт продавца, если нужен."),
                "date": _str("Дата заказа YYYY-MM-DD или полная ISO-дата. По умолчанию сейчас."),
            },
            "required": ["customer_name"],
            "additionalProperties": False,
        },
    },
    {
        "name": "update_order",
        "title": "Править ручной заказ",
        "description": "Изменить поля ручного заказа. Заказы из магазинов править нельзя — их перезапишет синк.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_id": _int("Внутренний id заказа."),
                "customer_name": _str("Имя и фамилия клиента."),
                "customer_phone": _str("Телефон."),
                "customer_email": _str("E-mail."),
                "customer_city": _str("Город."),
                "customer_address": _str("Адрес."),
                "payment_method": _str("Способ оплаты."),
                "product_type": _str("Тип товара."),
                "configuration": _str("Комплектация."),
                "comment": _str("Комментарий."),
                "status": _str("Статус."),
                "price": _num("Цена товара, PLN."),
                "shipping_cost": _num("Доставка, PLN."),
            },
            "required": ["order_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "delete_order",
        "title": "Удалить ручной заказ",
        "description": "Удалить заказ, созданный вручную. Заказы из магазинов удалять нельзя.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_id": _int("Внутренний id заказа."),
                "confirm": _bool("Должно быть true — защита от случайного удаления."),
            },
            "required": ["order_id", "confirm"],
            "additionalProperties": False,
        },
        "annotations": {"destructiveHint": True},
    },
    {
        "name": "run_sync",
        "title": "Запустить синхронизацию",
        "description": "Забрать свежие заказы из магазинов. Работает только если у инстанса включён SYNC_ENABLED.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source": _str("Магазин: dobraszklarnia, oteko, cieplarnia, allegro, erli. Пусто — все."),
                "full": _bool("true — полная пересинхронизация, а не только свежие."),
            },
            "additionalProperties": False,
        },
    },
]

READONLY_TOOLS = {"list_orders", "get_order", "stats", "sync_status"}


# ══════════════════════════════════════════════════════════════════════════
#  РЕАЛИЗАЦИЯ ИНСТРУМЕНТОВ
# ══════════════════════════════════════════════════════════════════════════
SEARCH_FIELDS = [
    "customer_name", "customer_phone", "customer_email",
    "external_id", "customer_city", "customer_address",
    "customer_comment", "items_json", "product_type",
    "invoice_nip", "seller_account",
]


async def _t_list_orders(a):
    conditions, params = ["1=1"], []

    src = (a.get("source") or "").strip()
    if src:
        lst = [s.strip() for s in src.split(",") if s.strip()]
        params.append(lst)
        conditions.append("source = ANY($%d)" % len(params))

    slr = (a.get("seller_account") or "").strip()
    if slr:
        params.append(slr)
        conditions.append("seller_account = $%d" % len(params))

    st = (a.get("status") or "").strip()
    if st:
        params.append(st)
        conditions.append("status = $%d" % len(params))

    q = (a.get("query") or "").strip()
    if q:
        for term in [t for t in q.split() if t][:6]:
            params.append("%" + term + "%")
            idx = len(params)
            parts = ["COALESCE(%s, '') ILIKE $%d" % (f, idx) for f in SEARCH_FIELDS]
            digits = "".join(ch for ch in term if ch.isdigit())
            if len(digits) >= 5:
                params.append("%" + digits + "%")
                parts.append(
                    "regexp_replace(COALESCE(customer_phone, ''), '[^0-9]', '', 'g') ILIKE $%d" % len(params)
                )
            conditions.append("(" + " OR ".join(parts) + ")")

    if a.get("date_from"):
        params.append(date.fromisoformat(a["date_from"]))
        conditions.append("COALESCE(external_created, created_at)::date >= $%d" % len(params))
    if a.get("date_to"):
        params.append(date.fromisoformat(a["date_to"]))
        conditions.append("COALESCE(external_created, created_at)::date <= $%d" % len(params))

    if a.get("exported") == "yes":
        conditions.append("exported = TRUE")
    elif a.get("exported") == "no":
        conditions.append("(exported IS NULL OR exported = FALSE)")
    if a.get("priority") == "yes":
        conditions.append("is_priority = TRUE")

    limit = max(1, min(int(a.get("limit") or 20), 100))
    offset = max(0, int(a.get("offset") or 0))
    where = " AND ".join(conditions)

    pool = await _pool()
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM orders WHERE " + where, *params)
        rows = await conn.fetch(
            "SELECT * FROM orders WHERE %s "
            "ORDER BY is_priority DESC NULLS LAST, COALESCE(external_created, created_at) DESC "
            "LIMIT $%d OFFSET $%d" % (where, len(params) + 1, len(params) + 2),
            *(params + [limit, offset])
        )
    return {
        "total": total,
        "returned": len(rows),
        "offset": offset,
        "orders": [_order_row(r) for r in rows],
    }


async def _t_get_order(a):
    oid, ext = a.get("order_id"), (a.get("external_id") or "").strip()
    if not oid and not ext:
        return {"error": "Нужен order_id или external_id"}
    pool = await _pool()
    async with pool.acquire() as conn:
        if oid:
            row = await conn.fetchrow("SELECT * FROM orders WHERE id=$1", int(oid))
        else:
            row = await conn.fetchrow(
                "SELECT * FROM orders WHERE external_id=$1 "
                "ORDER BY id DESC LIMIT 1", ext)
    if not row:
        return {"error": "Заказ не найден"}
    return _order_row(row, with_items=True)


async def _t_stats(a):
    today = date.today()
    df = date.fromisoformat(a["date_from"]) if a.get("date_from") else today.replace(day=1)
    dt = date.fromisoformat(a["date_to"]) if a.get("date_to") else today
    params = [df, dt]
    where = "COALESCE(external_created, created_at)::date BETWEEN $1 AND $2"
    if a.get("source"):
        params.append(a["source"].strip())
        where += " AND source = $3"

    pool = await _pool()
    async with pool.acquire() as conn:
        tot = await conn.fetchrow(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(total),0) AS revenue, "
            "COALESCE(AVG(total),0) AS avg_check FROM orders WHERE " + where, *params)
        by_src = await conn.fetch(
            "SELECT source, COUNT(*) AS cnt, COALESCE(SUM(total),0) AS revenue "
            "FROM orders WHERE " + where + " GROUP BY source ORDER BY revenue DESC", *params)
        by_st = await conn.fetch(
            "SELECT status, COUNT(*) AS cnt FROM orders WHERE " + where +
            " GROUP BY status ORDER BY cnt DESC", *params)
    return {
        "period": {"from": df.isoformat(), "to": dt.isoformat()},
        "orders": tot["cnt"],
        "revenue_pln": round(float(tot["revenue"]), 2),
        "avg_check_pln": round(float(tot["avg_check"]), 2),
        "by_source": [
            {"source": r["source"], "orders": r["cnt"], "revenue_pln": round(float(r["revenue"]), 2)}
            for r in by_src
        ],
        "by_status": [{"status": r["status"], "orders": r["cnt"]} for r in by_st],
    }


async def _t_sync_status(a):
    limit = max(1, min(int(a.get("limit") or 10), 50))
    pool = await _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT source, status, orders_new, orders_upd, error, started_at, finished_at "
            "FROM sync_log ORDER BY id DESC LIMIT $1", limit)
    return {
        "sync_enabled": bool(_DEPS.get("sync_enabled")),
        "instance": _DEPS.get("instance_id"),
        "log": [{k: _val(r[k]) for k in r.keys()} for r in rows],
    }


async def _order_exists(conn, oid):
    return await conn.fetchrow("SELECT id, external_id, source FROM orders WHERE id=$1", int(oid))


async def _t_set_comment(a):
    pool = await _pool()
    async with pool.acquire() as conn:
        if not await _order_exists(conn, a["order_id"]):
            return {"error": "Заказ не найден"}
        await conn.execute("UPDATE orders SET customer_comment=$1, updated_at=NOW() WHERE id=$2",
                           a.get("comment") or "", int(a["order_id"]))
    return {"ok": True, "order_id": int(a["order_id"]), "comment": a.get("comment") or ""}


async def _t_set_status(a):
    pool = await _pool()
    async with pool.acquire() as conn:
        row = await _order_exists(conn, a["order_id"])
        if not row:
            return {"error": "Заказ не найден"}
        await conn.execute("UPDATE orders SET status=$1, updated_at=NOW() WHERE id=$2",
                           (a.get("status") or "").strip(), int(a["order_id"]))
    note = None
    if not (row["external_id"] or "").startswith("manual"):
        note = "Статус изменён только в панели. В магазин он не уходит и может быть перезаписан полной синхронизацией."
    return {"ok": True, "order_id": int(a["order_id"]), "status": a.get("status"), "note": note}


async def _t_set_priority(a):
    pool = await _pool()
    async with pool.acquire() as conn:
        if not await _order_exists(conn, a["order_id"]):
            return {"error": "Заказ не найден"}
        await conn.execute("UPDATE orders SET is_priority=$1 WHERE id=$2",
                           bool(a.get("is_priority")), int(a["order_id"]))
    return {"ok": True, "order_id": int(a["order_id"]), "is_priority": bool(a.get("is_priority"))}


async def _t_set_exported(a):
    flag = bool(a.get("exported"))
    pool = await _pool()
    async with pool.acquire() as conn:
        if not await _order_exists(conn, a["order_id"]):
            return {"error": "Заказ не найден"}
        await conn.execute("UPDATE orders SET exported=$1, exported_at=$2 WHERE id=$3",
                           flag, datetime.now() if flag else None, int(a["order_id"]))
    return {"ok": True, "order_id": int(a["order_id"]), "exported": flag}


async def _t_create_order(a):
    ext_id = "manual-" + uuid.uuid4().hex[:8]
    tz = _DEPS.get("tz")
    when = datetime.now(tz) if tz else datetime.now()
    if a.get("date"):
        try:
            when = datetime.fromisoformat(a["date"])
        except Exception:
            pass

    price = float(a.get("price") or 0)
    ship = float(a.get("shipping_cost") or 0)
    total = price + ship
    items = json.dumps(
        [{"name": a.get("product_name") or "", "qty": 1, "price": str(price), "sku": ""}],
        ensure_ascii=False,
    )
    comment = a.get("comment") or ""

    pool = await _pool()
    async with pool.acquire() as conn:
        new_id = await conn.fetchval("""
            INSERT INTO orders (
                external_id, source, status, customer_name, customer_email,
                customer_phone, customer_city, customer_zip, customer_address,
                total, currency, items_count, items_json,
                payment_method, shipping_method, note, external_created,
                shipping_cost, seller_account, customer_comment,
                product_type, configuration, created_by_email
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'PLN',1,$11,$12,'',$13,$14,$15,$16,$17,$18,$19,$20)
            RETURNING id
        """,
            ext_id, (a.get("source") or "manual").strip(),
            (a.get("status") or "processing").strip(),
            a.get("customer_name") or "", a.get("customer_email") or "",
            a.get("customer_phone") or "", a.get("customer_city") or "", "",
            a.get("customer_address") or "", total, items,
            a.get("payment_method") or "", comment, when, ship,
            a.get("seller_account") or "", comment,
            a.get("product_type") or "", a.get("configuration") or "",
            "mcp",
        )
    return {"ok": True, "order_id": new_id, "external_id": ext_id, "total_pln": total}


UPDATABLE = {
    "customer_name": "customer_name", "customer_phone": "customer_phone",
    "customer_email": "customer_email", "customer_city": "customer_city",
    "customer_address": "customer_address", "payment_method": "payment_method",
    "product_type": "product_type", "configuration": "configuration",
    "status": "status",
}


async def _t_update_order(a):
    oid = int(a["order_id"])
    pool = await _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT external_id, total, shipping_cost FROM orders WHERE id=$1", oid)
        if not row:
            return {"error": "Заказ не найден"}
        if not (row["external_id"] or "").startswith("manual"):
            return {"error": "Это заказ из магазина — его перезапишет синхронизация. "
                             "Менять можно только комментарий, звезду, отметку выгрузки и статус."}
        sets, params = [], []
        for key, col in UPDATABLE.items():
            if key in a and a[key] is not None:
                params.append(a[key])
                sets.append("%s=$%d" % (col, len(params)))
        if "comment" in a and a["comment"] is not None:
            params.append(a["comment"])
            sets.append("customer_comment=$%d" % len(params))
            params.append(a["comment"])
            sets.append("note=$%d" % len(params))
        if "price" in a or "shipping_cost" in a:
            ship = float(a["shipping_cost"]) if a.get("shipping_cost") is not None else float(row["shipping_cost"] or 0)
            if a.get("price") is not None:
                price = float(a["price"])
            else:
                price = float(row["total"] or 0) - float(row["shipping_cost"] or 0)
            params.append(ship)
            sets.append("shipping_cost=$%d" % len(params))
            params.append(price + ship)
            sets.append("total=$%d" % len(params))
        if not sets:
            return {"error": "Нечего менять — не передано ни одно поле"}
        params.append(oid)
        await conn.execute(
            "UPDATE orders SET %s, updated_at=NOW() WHERE id=$%d" % (", ".join(sets), len(params)),
            *params)
        fresh = await conn.fetchrow("SELECT * FROM orders WHERE id=$1", oid)
    return {"ok": True, "order": _order_row(fresh, with_items=True)}


async def _t_delete_order(a):
    if not a.get("confirm"):
        return {"error": "Нужен confirm=true"}
    oid = int(a["order_id"])
    pool = await _pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT external_id, customer_name FROM orders WHERE id=$1", oid)
        if not row:
            return {"error": "Заказ не найден"}
        if not (row["external_id"] or "").startswith("manual"):
            return {"error": "Удалять можно только ручные заказы"}
        await conn.execute("DELETE FROM orders WHERE id=$1", oid)
    return {"ok": True, "deleted_order_id": oid, "customer": row["customer_name"]}


async def _t_run_sync(a):
    if not _DEPS.get("sync_enabled"):
        return {"error": "На этом инстансе синхронизация выключена (SYNC_ENABLED=false). "
                         "Внешние API не вызываются — это защита боевого аккаунта."}
    sync_map = _DEPS.get("sync_map") or {}
    src = (a.get("source") or "").strip().lower()
    full = bool(a.get("full"))
    targets = [src] if src else list(sync_map.keys())
    result = {}
    for name in targets:
        fn = sync_map.get(name)
        if not fn:
            result[name] = "неизвестный магазин"
            continue
        try:
            await fn(full=full)
            result[name] = "ok"
        except Exception as e:
            result[name] = "ошибка: %s" % e
    return {"ok": True, "full": full, "result": result}


HANDLERS = {
    "list_orders": _t_list_orders,
    "get_order": _t_get_order,
    "stats": _t_stats,
    "sync_status": _t_sync_status,
    "set_comment": _t_set_comment,
    "set_status": _t_set_status,
    "set_priority": _t_set_priority,
    "set_exported": _t_set_exported,
    "create_order": _t_create_order,
    "update_order": _t_update_order,
    "delete_order": _t_delete_order,
    "run_sync": _t_run_sync,
}


# ══════════════════════════════════════════════════════════════════════════
#  JSON-RPC
# ══════════════════════════════════════════════════════════════════════════
def _ok(rpc_id, result):
    return JSONResponse({"jsonrpc": "2.0", "id": rpc_id, "result": result})


def _err(rpc_id, code, message, status=200):
    return JSONResponse(
        {"jsonrpc": "2.0", "id": rpc_id, "error": {"code": code, "message": message}},
        status_code=status,
    )


def _visible_tools():
    if _readonly():
        return [t for t in TOOLS if t["name"] in READONLY_TOOLS]
    return TOOLS


def _authorized(request: Request, path_token: str) -> bool:
    secret = _token()
    if not secret:
        return False
    if path_token and path_token == secret:
        return True
    hdr = request.headers.get("authorization") or ""
    if hdr.lower().startswith("bearer ") and hdr[7:].strip() == secret:
        return True
    if (request.headers.get("x-api-key") or "").strip() == secret:
        return True
    return False


async def _handle(request: Request, path_token: str = ""):
    if not _token():
        return JSONResponse(
            {"error": "MCP выключен: не задана переменная окружения MCP_TOKEN"},
            status_code=503,
        )
    if not _authorized(request, path_token):
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    try:
        body = await request.json()
    except Exception:
        return _err(None, -32700, "Parse error")

    batch = body if isinstance(body, list) else [body]
    responses = []
    for msg in batch:
        if not isinstance(msg, dict):
            continue
        rpc_id = msg.get("id")
        method = msg.get("method") or ""
        params = msg.get("params") or {}

        # Уведомления (без id) — ответа не требуют
        if rpc_id is None and method.startswith("notifications/"):
            continue

        if method == "initialize":
            want = (params.get("protocolVersion") or "").strip()
            proto = want if want in SUPPORTED_PROTOCOLS else PROTOCOL_VERSION
            responses.append({
                "jsonrpc": "2.0", "id": rpc_id,
                "result": {
                    "protocolVersion": proto,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {
                        "name": "ekomat-dashboard",
                        "title": "EKOMAT Dashboard",
                        "version": "1.0.0",
                    },
                    "instructions": (
                        "Панель заказов EKOMAT (инстанс: %s). Через list_orders ищи заказы, "
                        "через get_order смотри детали. Править можно комментарий, статус, "
                        "звезду и отметку выгрузки у любого заказа; создавать, менять и удалять "
                        "целиком — только ручные заказы. Заказы из магазинов перезаписываются "
                        "синхронизацией, поэтому их поля менять нельзя."
                        % _DEPS.get("instance_id")
                    ),
                },
            })
        elif method == "ping":
            responses.append({"jsonrpc": "2.0", "id": rpc_id, "result": {}})
        elif method == "tools/list":
            responses.append({"jsonrpc": "2.0", "id": rpc_id,
                              "result": {"tools": _visible_tools()}})
        elif method in ("resources/list", "resources/templates/list"):
            key = "resourceTemplates" if method.endswith("templates/list") else "resources"
            responses.append({"jsonrpc": "2.0", "id": rpc_id, "result": {key: []}})
        elif method == "prompts/list":
            responses.append({"jsonrpc": "2.0", "id": rpc_id, "result": {"prompts": []}})
        elif method == "tools/call":
            name = params.get("name") or ""
            args = params.get("arguments") or {}
            fn = HANDLERS.get(name)
            if fn is None:
                responses.append({"jsonrpc": "2.0", "id": rpc_id,
                                  "error": {"code": -32602, "message": "Неизвестный инструмент: %s" % name}})
            elif _readonly() and name not in READONLY_TOOLS:
                responses.append({
                    "jsonrpc": "2.0", "id": rpc_id,
                    "result": {"content": [{"type": "text",
                                            "text": "MCP_READONLY=true — изменять данные запрещено."}],
                               "isError": True},
                })
            else:
                try:
                    data = await fn(args)
                    is_err = isinstance(data, dict) and "error" in data
                    responses.append({
                        "jsonrpc": "2.0", "id": rpc_id,
                        "result": {
                            "content": [{"type": "text",
                                         "text": json.dumps(data, ensure_ascii=False, indent=2)}],
                            "structuredContent": data if isinstance(data, dict) else {"result": data},
                            "isError": bool(is_err),
                        },
                    })
                except Exception as e:
                    print("[mcp] tool %s failed: %s\n%s" % (name, e, traceback.format_exc()))
                    responses.append({
                        "jsonrpc": "2.0", "id": rpc_id,
                        "result": {"content": [{"type": "text", "text": "Ошибка инструмента: %s" % e}],
                                   "isError": True},
                    })
        else:
            responses.append({"jsonrpc": "2.0", "id": rpc_id,
                              "error": {"code": -32601, "message": "Метод не поддерживается: %s" % method}})

    if not responses:
        return Response(status_code=202)
    payload = responses if isinstance(body, list) else responses[0]
    return JSONResponse(payload)


@mcp_router.post("/mcp")
async def mcp_root(request: Request):
    return await _handle(request, "")


@mcp_router.post("/mcp/{path_token}")
async def mcp_with_token(request: Request, path_token: str):
    return await _handle(request, path_token)


@mcp_router.get("/mcp")
@mcp_router.get("/mcp/{path_token}")
async def mcp_get(request: Request, path_token: str = ""):
    """Клиенты иногда пробуют открыть SSE-поток. Потока нет — сервер без состояния."""
    if not _token():
        return JSONResponse({"error": "MCP выключен: не задана MCP_TOKEN"}, status_code=503)
    if not _authorized(request, path_token):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return JSONResponse({"error": "Method Not Allowed: сервер работает без SSE-потока"},
                        status_code=405)


@mcp_router.delete("/mcp")
@mcp_router.delete("/mcp/{path_token}")
async def mcp_delete(request: Request, path_token: str = ""):
    return Response(status_code=204)
