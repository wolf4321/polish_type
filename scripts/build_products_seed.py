"""
Собирает каталог товаров из Excel-файлов клиента в data/products_seed.json.

Запуск (из корня репо):
    python3 scripts/build_products_seed.py

Источники (лежат в data/):
    src_poliweglan.xlsx      — поликарбонат коморовый и литой: вес, EAN OTEKO, EAN EKOMAT
    src_szklarnie.xlsx       — теплицы: EAN EKOMAT (1–2 шт.), EAN OTEKO (1–2 шт.)
    src_profile_tasmy.xlsx   — профили/аксессуары (EAN EKOMAT, EAN OTEKO) и ленты (без кодов)

Сид заливается в базу только если таблица products пустая (см. main.py, _seed_products).
Дальше каталог правят в интерфейсе (/products).
"""
import json
import re
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

EAN_RE = re.compile(r"^\d{13}$")


def ean(v):
    if v is None:
        return None
    s = str(v).strip()
    if isinstance(v, float) and v.is_integer():
        s = str(int(v))
    return s if EAN_RE.match(s) else None


def num(v):
    try:
        f = float(v)
        return round(f, 3) if f > 0 else None
    except (TypeError, ValueError):
        return None


COLOR_NORM = {
    "bezbarwny": "Bezbarwny",
    "grafit": "Grafit",
    "dymiony brąz": "Dymiony brąz",
    "mleczny": "Mleczny",
}

products = []
seen_names = set()


def add(p):
    key = p["name"].strip().lower()
    if key in seen_names:
        return
    seen_names.add(key)
    p["sort"] = len(products) + 1
    products.append(p)


# ── 1. Поликарбонат ─────────────────────────────────────────
PC_RE = re.compile(r"^Poliwęglan (komorowy|lity) (.+?) (\d+)mm (\d+)x(\d+)$", re.I)
wb = openpyxl.load_workbook(DATA / "src_poliweglan.xlsx", data_only=True)
for ws in wb:
    for r in ws.iter_rows(values_only=True):
        name = r[1] if len(r) > 1 else None
        if not isinstance(name, str):
            continue
        name = " ".join(name.split())
        m = PC_RE.match(name)
        if not m:
            continue
        kind, color, thick, w, l = m.groups()
        color = COLOR_NORM.get(color.lower(), color.strip().capitalize())
        codes = []
        if ean(r[6]):
            codes.append({"code": ean(r[6]), "label": "OTEKO"})
        if len(r) > 7 and ean(r[7]):
            codes.append({"code": ean(r[7]), "label": "EKOMAT"})
        add({
            "name": name,
            "category": "Poliwęglan " + kind.lower(),
            "product_type": "Поликарбонат",
            "attrs": {
                "Grubość": f"{thick} mm",
                "Kolor": color,
                "Wymiar": f"{w}x{l}",
            },
            "weight_kg": num(r[5]),
            "codes": codes,
            # Позиции без кодов клиент пока не продаёт — скрыты, но есть в каталоге
            "active": bool(codes),
        })

# ── 2. Теплицы ──────────────────────────────────────────────
SZ_RE = re.compile(
    r"^Szklarnia (.+?) (?:(\d+(?:,\d+)?m) )?(\d+(?:,\d+)?x\d+) (\d+)mm( NANO)?$", re.I)
wb = openpyxl.load_workbook(DATA / "src_szklarnie.xlsx", data_only=True)
ws = wb.active
labels = ["EKOMAT", "EKOMAT", "OTEKO", "OTEKO"]
for r in ws.iter_rows(min_row=2, values_only=True):
    name = r[0]
    if not isinstance(name, str) or not name.strip().lower().startswith("szklarnia "):
        continue
    name = " ".join(name.split())
    codes = []
    for i, lab in enumerate(labels, start=1):
        c = ean(r[i]) if len(r) > i else None
        if c and c not in [x["code"] for x in codes]:
            codes.append({"code": c, "label": lab})
    attrs = {}
    m = SZ_RE.match(name)
    if m:
        model, var, size, thick, nano = m.groups()
        attrs["Model"] = model.upper()
        if var:
            attrs["Wariant"] = var
        attrs["Wymiar"] = size
        attrs["Grubość"] = f"{thick} mm"
        attrs["NANO"] = "NANO" if nano else "—"
    add({
        "name": name,
        "category": "Szklarnie",
        "product_type": "Теплица",
        "attrs": attrs,
        "weight_kg": None,
        "codes": codes,
        "active": True,
    })

# ── 3. Профили, аксессуары, ленты ───────────────────────────
wb = openpyxl.load_workbook(DATA / "src_profile_tasmy.xlsx", data_only=True)
for ws in wb:
    is_tasmy = ws.title.strip().lower().startswith("taśm")
    for r in ws.iter_rows(min_row=2, values_only=True):
        name = r[0]
        if not isinstance(name, str) or len(name.strip()) < 12:
            continue
        name = " ".join(name.split())
        codes = []
        if len(r) > 1 and ean(r[1]):
            codes.append({"code": ean(r[1]), "label": "EKOMAT"})
        if len(r) > 2 and ean(r[2]):
            codes.append({"code": ean(r[2]), "label": "OTEKO"})
        add({
            "name": name,
            "category": "Taśmy" if is_tasmy else "Profile i akcesoria",
            "product_type": "Аксессуары",
            "attrs": {},
            "weight_kg": None,
            "codes": codes,
            "active": True,
        })

# ── Отчёт ───────────────────────────────────────────────────
from collections import Counter

code_cnt = Counter(c["code"] for p in products for c in p["codes"])
dups = {c: [p["name"] for p in products if any(x["code"] == c for x in p["codes"])]
        for c, n in code_cnt.items() if n > 1}

out = DATA / "products_seed.json"
out.write_text(json.dumps(products, ensure_ascii=False, indent=1), encoding="utf-8")

by_cat = Counter(p["category"] for p in products)
print(f"products: {len(products)}  ->  {out.relative_to(ROOT)}")
for c, n in by_cat.items():
    act = sum(1 for p in products if p["category"] == c and p["active"])
    w = sum(1 for p in products if p["category"] == c and p["weight_kg"])
    print(f"  {c:24s} {n:4d}  active {act:4d}  with weight {w:4d}")
print("codes:", sum(code_cnt.values()), "unique:", len(code_cnt))
print("duplicate codes (auto-match disabled for them):")
for c, names in dups.items():
    print("  ", c, names)
