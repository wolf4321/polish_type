"""
Каталог товаров (продуктизация).

Каждый товар — одно фиксированное название + вес за штуку + сколько угодно кодов
(EAN OTEKO, EAN EKOMAT, коды Allegro и т.д.). Один товар может иметь много кодов,
код — один товар. Если один и тот же код по ошибке стоит у двух товаров,
автоподбор по нему выключен (программа не угадывает), товар выбирают руками.

Где используется:
  * ручной заказ — менеджер выбирает товар кнопками, название и вес подставляются;
  * заказы из магазинов/Allegro — если в позиции пришёл код из каталога,
    название берётся из каталога, вес считается (см. apply_catalog);
  * колонка «Waga» в таблице заказов и в выгрузке XLSX.

Страница редактирования: /products (право "products").
"""
import json
import re
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_user, has_perm

router = APIRouter()
templates = Jinja2Templates(directory="templates")

_get_pool = None  # проставляется из main.py: catalog.configure(get_db_pool)

SEED_FILE = Path(__file__).resolve().parent / "data" / "products_seed.json"

# Порядок кнопок-параметров по категориям (что не указано — по алфавиту)
ATTR_ORDER = ["Model", "Wariant", "Grubość", "Kolor", "Wymiar", "NANO"]

CODE_RE = re.compile(r"\d{8,14}")


def configure(get_pool):
    global _get_pool
    _get_pool = get_pool


# ============================================================
# Таблицы + сид
# ============================================================

async def init_catalog_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id            SERIAL PRIMARY KEY,
            name          TEXT NOT NULL,
            category      VARCHAR(100) DEFAULT '',
            product_type  VARCHAR(100) DEFAULT '',
            attrs         TEXT DEFAULT '{}',
            weight_kg     NUMERIC(10,3),
            active        BOOLEAN DEFAULT TRUE,
            sort          INTEGER DEFAULT 0,
            created_at    TIMESTAMP DEFAULT NOW(),
            updated_at    TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS product_codes (
            id          SERIAL PRIMARY KEY,
            product_id  INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            code        VARCHAR(64) NOT NULL,
            label       VARCHAR(50) DEFAULT '',
            UNIQUE(product_id, code)
        )
    """)
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_product_codes_code ON product_codes (code)")
    await conn.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS weight_kg NUMERIC(10,2)")

    n = await conn.fetchval("SELECT COUNT(*) FROM products")
    if n == 0 and SEED_FILE.exists():
        await _seed(conn)
    await _apply_ean_updates(conn)
    # Типы в каталоге — по-польски, как в заказах (идемпотентно)
    for old, new in (("Теплица", "Szklarnia"), ("Поликарбонат", "Poliwęglan"),
                     ("Аксессуары", "Akcesoria")):
        await conn.execute("UPDATE products SET product_type=$1 WHERE product_type=$2", new, old)


# Обновления кодов от клиента: файл data/ean_update_*.json — источник правды
# для перечисленных в нём товаров (их коды заменяются целиком: так исправлены
# прежние дубли и ошибки). Товары, которых нет в каталоге, добавляются.
# Каждый файл применяется один раз (флаг в app_settings).
EAN_UPDATES = sorted((Path(__file__).resolve().parent / "data").glob("ean_update_*.json"))


def _name_key(n: str) -> str:
    return re.sub(r"\s+", "", (n or "").lower())


async def _apply_ean_updates(conn):
    for f in EAN_UPDATES:
        flag = "migration:" + f.stem
        if await conn.fetchval("SELECT value FROM app_settings WHERE key=$1", flag):
            continue
        data = json.loads(f.read_text(encoding="utf-8"))
        rows = await conn.fetch("SELECT id, name FROM products")
        by_name = {_name_key(r["name"]): r["id"] for r in rows}
        upd = new = 0
        async with conn.transaction():
            sort = await conn.fetchval("SELECT COALESCE(MAX(sort),0) FROM products")
            for item in data:
                pid = by_name.get(_name_key(item["name"]))
                if not pid:
                    sort += 1
                    pid = await conn.fetchval("""
                        INSERT INTO products (name, category, product_type, attrs, active, sort)
                        VALUES ($1,$2,$3,'{}',TRUE,$4) RETURNING id
                    """, item["name"], item.get("category", ""), item.get("product_type", ""), sort)
                    by_name[_name_key(item["name"])] = pid
                    new += 1
                else:
                    upd += 1
                    await conn.execute(
                        "UPDATE products SET active=TRUE, updated_at=NOW() WHERE id=$1", pid)
                await _save_codes(conn, pid, item["codes"])
            await conn.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES ($1,$2,NOW()) "
                "ON CONFLICT (key) DO NOTHING", flag, "updated=%d new=%d" % (upd, new))
        print(f"[catalog] {f.name}: обновлено {upd}, добавлено {new}")


async def _seed(conn):
    data = json.loads(SEED_FILE.read_text(encoding="utf-8"))
    async with conn.transaction():
        for p in data:
            pid = await conn.fetchval("""
                INSERT INTO products (name, category, product_type, attrs, weight_kg, active, sort)
                VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id
            """, p["name"], p.get("category", ""), p.get("product_type", ""),
                json.dumps(p.get("attrs") or {}, ensure_ascii=False),
                p.get("weight_kg"), bool(p.get("active", True)), int(p.get("sort", 0)))
            for c in p.get("codes") or []:
                await conn.execute(
                    "INSERT INTO product_codes (product_id, code, label) VALUES ($1,$2,$3) "
                    "ON CONFLICT DO NOTHING", pid, c["code"], c.get("label", ""))
    print(f"[catalog] seeded {len(data)} products from {SEED_FILE.name}")


# ============================================================
# Сопоставление позиций заказа с каталогом
# ============================================================

def _f(v):
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _item_codes(it: dict) -> list:
    """Коды, по которым ищем позицию: sku, ean, gtin, code и цифры из них."""
    out = []
    for k in ("code", "ean", "gtin", "sku", "external_code"):
        v = it.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        out.append(s)
        out.extend(CODE_RE.findall(s))
    seen, res = set(), []
    for c in out:
        if c not in seen:
            seen.add(c)
            res.append(c)
    return res


async def lookup_codes(conn, codes: list) -> dict:
    """code -> product (только однозначные коды)."""
    if not codes:
        return {}
    rows = await conn.fetch("""
        SELECT pc.code, p.id, p.name, p.weight_kg, p.category, p.product_type
        FROM product_codes pc JOIN products p ON p.id = pc.product_id
        WHERE pc.code = ANY($1::text[])
    """, list(codes))
    by_code = {}
    for r in rows:
        by_code.setdefault(r["code"], []).append(r)
    return {c: dict(rs[0]) for c, rs in by_code.items() if len({x["id"] for x in rs}) == 1}


def order_weight(items: list):
    """Вес заказа = сумма (вес за шт × кол-во). Позиции без веса не считаются.
    Возвращает (вес или None, все_ли_позиции_с_весом)."""
    total, complete, any_w = 0.0, True, False
    for it in items:
        w = _f(it.get("weight"))
        try:
            q = float(it.get("qty") or 1)
        except (TypeError, ValueError):
            q = 1
        if w is None:
            complete = False
            continue
        any_w = True
        total += w * q
    return (round(total, 2) if any_w else None), complete


async def apply_catalog(conn, items: list) -> list:
    """Для заказов из магазинов: если код позиции есть в каталоге — подставляем
    название и вес из каталога. Исходное название сохраняем в orig_name."""
    want = []
    for it in items:
        if it.get("product_id"):
            continue
        want.extend(_item_codes(it))
    found = await lookup_codes(conn, want) if want else {}
    for it in items:
        if it.get("product_id"):
            continue
        for c in _item_codes(it):
            p = found.get(c)
            if not p:
                continue
            if not it.get("orig_name"):
                it["orig_name"] = it.get("full_name") or it.get("name") or ""
            it["name"] = p["name"]
            it["full_name"] = p["name"]
            it["product_id"] = p["id"]
            it["code"] = c
            if p["weight_kg"] is not None and _f(it.get("weight")) is None:
                it["weight"] = float(p["weight_kg"])
            break
    return items


async def apply_catalog_to_order(conn, source: str, external_id: str):
    """Вызывается после сохранения заказа из синка."""
    row = await conn.fetchrow(
        "SELECT id, items_json FROM orders WHERE source=$1 AND external_id=$2",
        source, str(external_id))
    if not row:
        return
    try:
        items = json.loads(row["items_json"] or "[]")
    except Exception:
        return
    if not isinstance(items, list) or not items:
        return
    before = json.dumps(items, ensure_ascii=False, sort_keys=True)
    items = await apply_catalog(conn, items)
    weight, _ = order_weight(items)
    after = json.dumps(items, ensure_ascii=False, sort_keys=True)
    if after != before or weight is not None:
        await conn.execute(
            "UPDATE orders SET items_json=$1, weight_kg=$2 WHERE id=$3",
            json.dumps(items, ensure_ascii=False), weight, row["id"])


# ============================================================
# API
# ============================================================

def _row(r, codes=None):
    try:
        attrs = json.loads(r["attrs"] or "{}")
    except Exception:
        attrs = {}
    return {
        "id": r["id"],
        "name": r["name"],
        "category": r["category"] or "",
        "product_type": r["product_type"] or "",
        "attrs": attrs,
        "weight_kg": float(r["weight_kg"]) if r["weight_kg"] is not None else None,
        "active": bool(r["active"]),
        "codes": codes or [],
    }


async def _all_products(conn, include_inactive=False):
    rows = await conn.fetch(
        "SELECT * FROM products " + ("" if include_inactive else "WHERE active ")
        + "ORDER BY sort, id")
    crow = await conn.fetch("SELECT product_id, code, label FROM product_codes ORDER BY id")
    codes = {}
    for c in crow:
        codes.setdefault(c["product_id"], []).append({"code": c["code"], "label": c["label"] or ""})
    dup = await conn.fetch(
        "SELECT code FROM product_codes GROUP BY code HAVING COUNT(DISTINCT product_id) > 1")
    dups = {d["code"] for d in dup}
    out = []
    for r in rows:
        cs = codes.get(r["id"], [])
        for c in cs:
            if c["code"] in dups:
                c["dup"] = True
        out.append(_row(r, cs))
    return out


@router.get("/products", response_class=HTMLResponse)
async def products_page(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)
    if not has_perm(user, "products"):
        return RedirectResponse("/orders", status_code=302)
    return templates.TemplateResponse(request=request, name="products.html",
                                      context={"user": user})


@router.get("/api/products")
async def api_products(request: Request, all: int = 0):
    """Каталог. Для формы заказа — только активные; all=1 — для страницы каталога."""
    user = await get_current_user(request)
    if not user or not (has_perm(user, "orders") or has_perm(user, "products")):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        items = await _all_products(conn, include_inactive=bool(all) and has_perm(user, "products"))
    return JSONResponse({"products": items, "attr_order": ATTR_ORDER})


@router.get("/api/products/lookup")
async def api_products_lookup(request: Request, code: str = ""):
    user = await get_current_user(request)
    if not user or not has_perm(user, "orders"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    code = (code or "").strip()
    if not code:
        return JSONResponse({"found": False})
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT p.id, p.name FROM product_codes pc JOIN products p ON p.id=pc.product_id
            WHERE pc.code=$1
        """, code)
    return JSONResponse({"found": len(rows) == 1, "ambiguous": len(rows) > 1,
                         "products": [dict(r) for r in rows]})


def _clean_body(body: dict):
    name = " ".join(str(body.get("name") or "").split())
    if not name:
        return None, "Nazwa jest wymagana"
    attrs = body.get("attrs") or {}
    if not isinstance(attrs, dict):
        attrs = {}
    attrs = {str(k).strip(): str(v).strip() for k, v in attrs.items()
             if str(k).strip() and str(v).strip()}
    codes = []
    for c in body.get("codes") or []:
        if isinstance(c, str):
            c = {"code": c, "label": ""}
        code = str(c.get("code") or "").strip()
        if code and code not in [x["code"] for x in codes]:
            codes.append({"code": code, "label": str(c.get("label") or "").strip()[:50]})
    w = body.get("weight_kg")
    w = _f(str(w).replace(",", ".")) if w not in (None, "") else None
    return {
        "name": name,
        "category": str(body.get("category") or "").strip()[:100],
        "product_type": str(body.get("product_type") or "").strip()[:100],
        "attrs": attrs,
        "weight_kg": w,
        "active": bool(body.get("active", True)),
        "codes": codes,
    }, None


async def _conflicts(conn, codes, pid=None):
    """Коды, которые уже стоят у другого товара (сохранять можно, но предупреждаем)."""
    if not codes:
        return []
    rows = await conn.fetch("""
        SELECT pc.code, p.name FROM product_codes pc JOIN products p ON p.id=pc.product_id
        WHERE pc.code = ANY($1::text[]) AND ($2::int IS NULL OR pc.product_id <> $2)
    """, [c["code"] for c in codes], pid)
    return [{"code": r["code"], "name": r["name"]} for r in rows]


async def _save_codes(conn, pid, codes):
    await conn.execute("DELETE FROM product_codes WHERE product_id=$1", pid)
    for c in codes:
        await conn.execute(
            "INSERT INTO product_codes (product_id, code, label) VALUES ($1,$2,$3) "
            "ON CONFLICT DO NOTHING", pid, c["code"], c["label"])


@router.post("/api/products")
async def api_product_create(request: Request):
    user = await get_current_user(request)
    if not user or not has_perm(user, "products"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    data, err = _clean_body(await request.json())
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=400)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            sort = await conn.fetchval("SELECT COALESCE(MAX(sort),0)+1 FROM products")
            pid = await conn.fetchval("""
                INSERT INTO products (name, category, product_type, attrs, weight_kg, active, sort)
                VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id
            """, data["name"], data["category"], data["product_type"],
                json.dumps(data["attrs"], ensure_ascii=False), data["weight_kg"],
                data["active"], sort)
            await _save_codes(conn, pid, data["codes"])
        conflicts = await _conflicts(conn, data["codes"], pid)
    return JSONResponse({"ok": True, "id": pid, "conflicts": conflicts})


@router.put("/api/products/{pid}")
async def api_product_update(pid: int, request: Request):
    user = await get_current_user(request)
    if not user or not has_perm(user, "products"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    data, err = _clean_body(await request.json())
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=400)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            r = await conn.execute("""
                UPDATE products SET name=$1, category=$2, product_type=$3, attrs=$4,
                    weight_kg=$5, active=$6, updated_at=NOW()
                WHERE id=$7
            """, data["name"], data["category"], data["product_type"],
                json.dumps(data["attrs"], ensure_ascii=False), data["weight_kg"],
                data["active"], pid)
            if r.endswith(" 0"):
                return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
            await _save_codes(conn, pid, data["codes"])
        conflicts = await _conflicts(conn, data["codes"], pid)
    return JSONResponse({"ok": True, "conflicts": conflicts})


@router.delete("/api/products/{pid}")
async def api_product_delete(pid: int, request: Request):
    """Удаление = скрыть (active=false). Старые заказы ссылаются на товар по id."""
    user = await get_current_user(request)
    if not user or not has_perm(user, "products"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE products SET active=FALSE, updated_at=NOW() WHERE id=$1", pid)
    return JSONResponse({"ok": True})
