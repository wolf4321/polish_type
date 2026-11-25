#!/usr/bin/env python3
"""
Восстановление базы EKOMAT из архива, сделанного backup.py.

Как пользоваться
----------------
1. Скачай архив из репозитория бэкапов. Если файлов несколько
   (…tar.gz.part01, .part02 …) — скачай все и склей:

     Linux/Mac:  cat ekomat-*.tar.gz.part* > ekomat.tar.gz
     Windows:    copy /b file.part01+file.part02 ekomat.tar.gz

2. Подними приложение на ПУСТОЙ базе один раз — оно само создаст
   все таблицы и индексы. Останови его.

3. Запусти:

     python scripts/restore_backup.py ekomat.tar.gz --dsn postgresql://...

   Посмотреть, что внутри, ничего не меняя:

     python scripts/restore_backup.py ekomat.tar.gz --list

Что делает
----------
Для каждой таблицы из архива: очищает её (TRUNCATE) и заливает CSV
через COPY. Затем поднимает счётчики id, чтобы новые записи не
натыкались на существующие.

Важно: токены Allegro в архиве затёрты. После восстановления введи
refresh-токен заново через POST /admin/allegro/set-refresh-token.
"""

import argparse
import asyncio
import io
import json
import os
import sys
import tarfile

try:
    import asyncpg
except ImportError:
    print("Нужен asyncpg:  pip install asyncpg")
    sys.exit(1)


# Порядок заливки: сначала независимые таблицы.
PREFERRED_ORDER = [
    "users", "app_settings", "customers", "orders", "order_items",
    "products", "product_codes", "kb_qa", "assistant_log",
    "leads", "sync_log", "export_log",
]


def _open(path):
    if not os.path.exists(path):
        print("Файл не найден: %s" % path)
        sys.exit(1)
    return tarfile.open(path, "r:gz")


def cmd_list(path):
    tar = _open(path)
    names = tar.getnames()
    if "manifest.json" in names:
        m = json.loads(tar.extractfile("manifest.json").read().decode("utf-8"))
        print("Создан:    %s" % m.get("created_at"))
        print("Инстанс:   %s" % m.get("instance"))
        print("Размер:    %.2f МБ" % ((m.get("archive_bytes") or 0) / 1048576.0))
        print("Таблицы:")
        for t, info in sorted(m.get("tables", {}).items()):
            print("   %-20s %8d строк" % (t, info.get("rows", 0)))
    else:
        print("manifest.json не найден, файлы внутри:")
        for n in names:
            print("  ", n)
    tar.close()


async def cmd_restore(path, dsn, only, yes):
    tar = _open(path)
    names = [n for n in tar.getnames() if n.endswith(".csv")]
    tables = [n[:-4] for n in names]
    if only:
        wanted = {t.strip() for t in only.split(",") if t.strip()}
        tables = [t for t in tables if t in wanted]
    if not tables:
        print("В архиве нет таблиц для восстановления.")
        return

    ordered = [t for t in PREFERRED_ORDER if t in tables]
    ordered += [t for t in sorted(tables) if t not in ordered]

    print("Будут ПЕРЕЗАПИСАНЫ таблицы: %s" % ", ".join(ordered))
    if not yes:
        ans = input("Продолжить? Все текущие данные в них пропадут. [yes/N] ").strip().lower()
        if ans != "yes":
            print("Отменено.")
            return

    conn = await asyncpg.connect(dsn)
    try:
        existing = {r["table_name"] for r in await conn.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'")}

        await conn.execute("SET session_replication_role = replica")
        for t in ordered:
            if t not in existing:
                print("  пропуск %-20s — такой таблицы в базе нет" % t)
                continue
            raw = tar.extractfile("%s.csv" % t).read()
            header = raw.split(b"\n", 1)[0].decode("utf-8").strip()
            cols = [c.strip().strip('"') for c in header.split(",")]
            await conn.execute('TRUNCATE TABLE "%s" CASCADE' % t)
            await conn.copy_to_table(
                t, source=io.BytesIO(raw), columns=cols,
                format="csv", header=True,
            )
            cnt = await conn.fetchval('SELECT COUNT(*) FROM "%s"' % t)
            print("  %-20s %8d строк" % (t, cnt))
        await conn.execute("SET session_replication_role = origin")

        # Счётчики id
        seqs = await conn.fetch("""
            SELECT c.relname AS seq, t.relname AS tbl, a.attname AS col
            FROM pg_class c
            JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a'
            JOIN pg_class t ON t.oid = d.refobjid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
            WHERE c.relkind = 'S'
        """)
        for s in seqs:
            if s["tbl"] not in ordered:
                continue
            await conn.execute(
                'SELECT setval(%s, COALESCE((SELECT MAX("%s") FROM "%s"), 1))'
                % ("'%s'" % s["seq"], s["col"], s["tbl"])
            )
        print("Счётчики id обновлены.")
    finally:
        await conn.close()
        tar.close()
    print("\nГотово. Не забудь заново ввести refresh-токен Allegro.")


def main():
    p = argparse.ArgumentParser(description="Восстановление базы EKOMAT из архива.")
    p.add_argument("archive", help="путь к ekomat-....tar.gz")
    p.add_argument("--dsn", default=os.getenv("DB_CONNECT") or os.getenv("DATABASE_URL"),
                   help="строка подключения к базе, куда восстанавливаем")
    p.add_argument("--only", help="восстановить только указанные таблицы, через запятую")
    p.add_argument("--list", action="store_true", help="показать содержимое и выйти")
    p.add_argument("--yes", action="store_true", help="не спрашивать подтверждение")
    a = p.parse_args()

    if a.list:
        cmd_list(a.archive)
        return
    if not a.dsn:
        print("Не задан --dsn (или переменная DB_CONNECT).")
        sys.exit(1)
    asyncio.run(cmd_restore(a.archive, a.dsn, a.only, a.yes))


if __name__ == "__main__":
    main()
