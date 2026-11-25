"""
EKOMAT — резервное копирование базы.

Что делает
----------
Раз в сутки (по умолчанию 03:30 по Варшаве) выгружает всю базу в один
сжатый архив .tar.gz и кладёт его в приватный репозиторий на GitHub.
Хранит N последних копий, лишние удаляет сама.

Архив внутри — обычные CSV-файлы, по одному на таблицу, плюс manifest.json
со списком таблиц и количеством строк. Такой архив читается чем угодно,
не требует pg_dump и восстанавливается скриптом scripts/restore_backup.py.

Переменные окружения
--------------------
  BACKUP_GITHUB_TOKEN   токен GitHub с правом писать в репозиторий бэкапов.
                        Без него выгрузка на GitHub выключена (но ручное
                        скачивание через админку продолжает работать).
  BACKUP_GITHUB_REPO    куда складывать, в формате "логин/название-репо",
                        например  wolf4321/ekomat-backups
  BACKUP_GITHUB_BRANCH  ветка, по умолчанию main
  BACKUP_KEEP           сколько копий хранить, по умолчанию 30
  BACKUP_HOUR           час запуска, по умолчанию 3
  BACKUP_MINUTE         минута запуска, по умолчанию 30
  BACKUP_ENABLED        0/false — полностью выключить ночной бэкап

Безопасность
------------
Секреты в архив НЕ попадают: значения в app_settings, похожие на токены
(allegro, token, secret, password, refresh), заменяются на [REDACTED].
Таблица сессий не выгружается — она одноразовая.
После восстановления токен Allegro вводится заново через
POST /admin/allegro/set-refresh-token.
"""

import os
import io
import re
import csv
import json
import base64
import tarfile
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

from auth import get_current_user, has_perm
import gdrive


backup_router = APIRouter()

# ------------------------------------------------------------------
# Зависимости прокидываются из main.py, чтобы не было кольцевого импорта
# ------------------------------------------------------------------
_DEPS = {
    "get_db_pool": None,     # async () -> asyncpg.Pool
    "http_session": None,    # () -> aiohttp.ClientSession
    "tz": None,
    "instance_id": "v2",
}


def configure_backup(**kwargs):
    _DEPS.update({k: v for k, v in kwargs.items() if v is not None})


# ------------------------------------------------------------------
# Настройки
# ------------------------------------------------------------------
GITHUB_API = "https://api.github.com"
BACKUP_DIR = "backups"

# Contents API у GitHub не любит большие файлы, поэтому режем архив
# на куски и собираем обратно при восстановлении.
CHUNK_BYTES = 20 * 1024 * 1024

SKIP_TABLES = {"sessions"}
SECRET_KEY_RE = re.compile(r"(token|secret|password|refresh|client_id|api_key)", re.I)


def _env(name, default=""):
    return (os.getenv(name) or default).strip()


def _gh_token():
    return _env("BACKUP_GITHUB_TOKEN")


def _gh_repo():
    return _env("BACKUP_GITHUB_REPO")


def _gh_branch():
    return _env("BACKUP_GITHUB_BRANCH", "main") or "main"


def _keep():
    try:
        return max(1, int(_env("BACKUP_KEEP", "30")))
    except ValueError:
        return 30


def _enabled():
    raw = os.getenv("BACKUP_ENABLED")
    if raw is not None and raw.strip().lower() in ("0", "false", "no", "off"):
        return False
    return bool((_gh_token() and _gh_repo()) or gdrive.configured())


def _now():
    tz = _DEPS.get("tz")
    return datetime.now(tz) if tz else datetime.now(timezone.utc)


def _stamp():
    return _now().strftime("%Y-%m-%d_%H-%M")


# ==================================================================
# Сборка архива
# ==================================================================

def _redact_app_settings(raw: bytes) -> bytes:
    """Затирает значения, похожие на секреты, в выгрузке app_settings."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        return raw
    header = rows[0]
    try:
        i_key = header.index("key")
        i_val = header.index("value")
    except ValueError:
        return raw
    for r in rows[1:]:
        if len(r) > max(i_key, i_val) and SECRET_KEY_RE.search(r[i_key] or ""):
            r[i_val] = "[REDACTED]"
    out = io.StringIO()
    csv.writer(out, lineterminator="\n").writerows(rows)
    return out.getvalue().encode("utf-8")


async def make_dump() -> tuple:
    """Собирает полный архив базы. Возвращает (имя файла, байты, manifest)."""
    get_pool = _DEPS.get("get_db_pool")
    if not get_pool:
        raise RuntimeError("backup: не подключён доступ к базе")

    pool = await get_pool()
    instance = _DEPS.get("instance_id") or "v2"
    started = _now()

    manifest = {
        "created_at": started.isoformat(),
        "instance": instance,
        "format": "csv-per-table",
        "note": "Секреты в app_settings заменены на [REDACTED]. "
                "Восстановление: scripts/restore_backup.py",
        "tables": {},
    }

    buf = io.BytesIO()
    tar = tarfile.open(fileobj=buf, mode="w:gz")
    mtime = int(started.timestamp())

    def _add(name: str, data: bytes):
        info = tarfile.TarInfo(name)
        info.size = len(data)
        info.mtime = mtime
        tar.addfile(info, io.BytesIO(data))

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_type='BASE TABLE' "
            "ORDER BY table_name"
        )
        tables = [r["table_name"] for r in rows if r["table_name"] not in SKIP_TABLES]

        # Схема — для справки при восстановлении вручную
        cols = await conn.fetch(
            "SELECT table_name, column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns WHERE table_schema='public' "
            "ORDER BY table_name, ordinal_position"
        )
        schema = {}
        for c in cols:
            schema.setdefault(c["table_name"], []).append({
                "column": c["column_name"],
                "type": c["data_type"],
                "nullable": c["is_nullable"] == "YES",
                "default": c["column_default"],
            })
        _add("schema.json", json.dumps(schema, ensure_ascii=False, indent=2).encode("utf-8"))

        for t in tables:
            out = io.BytesIO()
            await conn.copy_from_query(
                'SELECT * FROM "%s"' % t, output=out, format="csv", header=True
            )
            data = out.getvalue()
            if t == "app_settings":
                data = _redact_app_settings(data)
            cnt = await conn.fetchval('SELECT COUNT(*) FROM "%s"' % t)
            manifest["tables"][t] = {"rows": int(cnt or 0), "bytes": len(data)}
            _add("%s.csv" % t, data)

    _add("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"))
    tar.close()

    blob = buf.getvalue()
    manifest["archive_bytes"] = len(blob)
    fname = "ekomat-%s-%s.tar.gz" % (instance, _stamp())
    return fname, blob, manifest


# ==================================================================
# GitHub
# ==================================================================

def _gh_headers():
    return {
        "Authorization": "Bearer %s" % _gh_token(),
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "ekomat-backup",
    }


def _session():
    factory = _DEPS.get("http_session")
    if not factory:
        raise RuntimeError("backup: не подключён HTTP-клиент")
    return factory()


async def _gh_list_dir(session, path: str):
    url = "%s/repos/%s/contents/%s?ref=%s" % (GITHUB_API, _gh_repo(), path, _gh_branch())
    async with session.get(url, headers=_gh_headers()) as r:
        if r.status == 404:
            return []
        if r.status >= 400:
            raise RuntimeError("GitHub %s: %s" % (r.status, (await r.text())[:300]))
        data = await r.json()
        return data if isinstance(data, list) else []


async def _gh_put(session, path: str, content: bytes, message: str):
    url = "%s/repos/%s/contents/%s" % (GITHUB_API, _gh_repo(), path)
    body = {
        "message": message,
        "content": base64.b64encode(content).decode("ascii"),
        "branch": _gh_branch(),
    }
    async with session.put(url, headers=_gh_headers(), json=body) as r:
        if r.status >= 400:
            raise RuntimeError("GitHub %s при записи %s: %s"
                               % (r.status, path, (await r.text())[:300]))
        return await r.json()


async def _gh_delete(session, path: str, sha: str, message: str):
    url = "%s/repos/%s/contents/%s" % (GITHUB_API, _gh_repo(), path)
    body = {"message": message, "sha": sha, "branch": _gh_branch()}
    async with session.request("DELETE", url, headers=_gh_headers(), json=body) as r:
        if r.status >= 400:
            raise RuntimeError("GitHub %s при удалении %s: %s"
                               % (r.status, path, (await r.text())[:300]))


def _split(blob: bytes):
    """Режет архив на куски. Один кусок — без суффикса."""
    if len(blob) <= CHUNK_BYTES:
        return [("", blob)]
    parts = []
    n = 0
    for i in range(0, len(blob), CHUNK_BYTES):
        n += 1
        parts.append((".part%02d" % n, blob[i:i + CHUNK_BYTES]))
    return parts


async def _cleanup_old(session, keep: int):
    """Оставляет keep последних копий, остальные удаляет."""
    files = await _gh_list_dir(session, BACKUP_DIR)
    groups = {}
    for f in files:
        name = f.get("name") or ""
        if not name.startswith("ekomat-"):
            continue
        base = name.split(".part")[0]
        groups.setdefault(base, []).append(f)
    if len(groups) <= keep:
        return 0
    old = sorted(groups.keys())[:len(groups) - keep]
    removed = 0
    for base in old:
        for f in groups[base]:
            await _gh_delete(session, "%s/%s" % (BACKUP_DIR, f["name"]), f["sha"],
                             "backup: очистка старой копии %s" % f["name"])
            removed += 1
    return removed


async def run_backup(reason: str = "scheduled") -> dict:
    """Собрать архив и положить на GitHub и/или Google Диск. Возвращает отчёт."""
    fname, blob, manifest = await make_dump()
    size_mb = round(len(blob) / 1048576.0, 2)
    report = {"file": fname, "size_mb": size_mb, "tables": manifest["tables"],
              "github": None, "gdrive": None}

    if _gh_token() and _gh_repo():
        try:
            parts = _split(blob)
            written = []
            session = _session()
            try:
                for suffix, chunk in parts:
                    path = "%s/%s%s" % (BACKUP_DIR, fname, suffix)
                    await _gh_put(session, path, chunk,
                                  "backup %s (%s)" % (fname + suffix, reason))
                    written.append(path)
                removed = await _cleanup_old(session, _keep())
            finally:
                await session.close()
            report["github"] = {"ok": True, "repo": _gh_repo(), "parts": len(written),
                                "removed_old_files": removed}
        except Exception as e:
            report["github"] = {"ok": False, "error": str(e)}

    if await gdrive.connected():
        try:
            report["gdrive"] = await gdrive.upload(fname, blob)
        except Exception as e:
            report["gdrive"] = {"ok": False, "error": str(e)}

    targets = [t for t in (report["github"], report["gdrive"]) if t]
    if not targets:
        report.update(ok=False, uploaded=False,
                      error="Brak miejsca docelowego: nie skonfigurowano GitHub "
                            "i nie połączono Google Drive — archiwum zbudowane, ale nie wysłane.")
        return report
    report["uploaded"] = any(t.get("ok") for t in targets)
    report["ok"] = all(t.get("ok") for t in targets)
    if not report["ok"]:
        report["error"] = "; ".join("%s: %s" % (n, t["error"])
                                    for n, t in (("GitHub", report["github"]), ("Drive", report["gdrive"]))
                                    if t and not t.get("ok"))
    print("[backup] %s — %s МБ, github=%s, gdrive=%s, причина: %s"
          % (fname, size_mb, report["github"] and report["github"].get("ok"),
             report["gdrive"] and report["gdrive"].get("ok"), reason))
    return report


async def scheduled_backup():
    """Точка входа для планировщика."""
    try:
        await run_backup(reason="по расписанию")
    except Exception as e:
        print("[backup] ОШИБКА: %s" % e)


# ==================================================================
# HTTP
# ==================================================================

async def _deny(request: Request):
    user = await get_current_user(request)
    if not user or not has_perm(user, "manage_users"):
        return JSONResponse({"error": "Tylko superadmin"}, status_code=403)
    return None


@backup_router.get("/admin/backup/status")
async def backup_status(request: Request):
    deny = await _deny(request)
    if deny:
        return deny
    out = {
        "enabled": _enabled(),
        "repo": _gh_repo() or None,
        "branch": _gh_branch(),
        "keep": _keep(),
        "schedule": "%02d:%02d" % (
            int(_env("BACKUP_HOUR", "3") or 3), int(_env("BACKUP_MINUTE", "30") or 30)),
        "instance": _DEPS.get("instance_id"),
        "files": [],
    }
    if _gh_token() and _gh_repo():
        session = _session()
        try:
            files = await _gh_list_dir(session, BACKUP_DIR)
        except Exception as e:
            out["error"] = str(e)
            files = []
        finally:
            await session.close()
        out["files"] = sorted(
            [{"name": f.get("name"),
              "size_mb": round((f.get("size") or 0) / 1048576.0, 2),
              "url": f.get("html_url")} for f in files if (f.get("name") or "").startswith("ekomat-")],
            key=lambda x: x["name"], reverse=True)[:60]
    return JSONResponse(out)


@backup_router.post("/admin/backup/run")
async def backup_run(request: Request):
    deny = await _deny(request)
    if deny:
        return deny
    try:
        return JSONResponse(await run_backup(reason="вручную из админки"))
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@backup_router.get("/admin/backup/download")
async def backup_download(request: Request):
    """Скачать свежий архив прямо в браузер, без GitHub."""
    deny = await _deny(request)
    if deny:
        return deny
    fname, blob, _ = await make_dump()
    return StreamingResponse(
        io.BytesIO(blob),
        media_type="application/gzip",
        headers={"Content-Disposition": 'attachment; filename="%s"' % fname},
    )
