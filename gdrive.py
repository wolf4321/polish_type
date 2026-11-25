"""
EKOMAT — вторая копия бэкапа на Google Диск.

Тот же архив, что уходит на GitHub (backup.make_dump), каждую ночь
заливается ещё и в папку на Google Диске владельца.

Подключение (один раз)
----------------------
1. В Google Cloud создать OAuth-клиент типа «Web application».
   Authorized redirect URI:  https://<домен панели>/admin/gdrive/callback
2. В Railway задать переменные:
     GDRIVE_CLIENT_ID
     GDRIVE_CLIENT_SECRET
     GDRIVE_REDIRECT_URI   (необязательно — по умолчанию строится из домена)
3. В панели: Użytkownicy → «Kopie zapasowe» → «Połącz Google Drive»,
   войти в Google и разрешить доступ.

Доступ запрашивается минимальный — drive.file: приложение видит только
файлы, которые само создало, остальной Диск ему недоступен.
refresh_token хранится в app_settings с префиксом инстанса и в архивы
не попадает (backup затирает ключи с token/refresh).

Хранение: последние GDRIVE_KEEP копий (по умолчанию 30) плюс первая
копия каждого месяца — без срока. Лишние уходят в корзину Диска.
"""

import os
import json
import re
import secrets
from datetime import datetime, timezone
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse

from auth import get_current_user, has_perm

gdrive_router = APIRouter()

_DEPS = {"get_db_pool": None, "http_session": None, "instance_id": "v2"}

AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
API = "https://www.googleapis.com/drive/v3"
UPLOAD = "https://www.googleapis.com/upload/drive/v3/files"
SCOPE = "https://www.googleapis.com/auth/drive.file"
FOLDER_MIME = "application/vnd.google-apps.folder"
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}_\d{2}-\d{2}")


def configure_gdrive(**kwargs):
    _DEPS.update({k: v for k, v in kwargs.items() if v is not None})


def _env(name, default=""):
    return (os.getenv(name) or default).strip()


def _client():
    return _env("GDRIVE_CLIENT_ID"), _env("GDRIVE_CLIENT_SECRET")


def configured() -> bool:
    cid, sec = _client()
    return bool(cid and sec)


def _keep():
    try:
        return max(1, int(_env("GDRIVE_KEEP", "30")))
    except ValueError:
        return 30


def _key(name):
    return "%s:gdrive_%s" % (_DEPS.get("instance_id") or "v2", name)


async def _get(name):
    pool = await _DEPS["get_db_pool"]()
    async with pool.acquire() as c:
        return await c.fetchval("SELECT value FROM app_settings WHERE key=$1", _key(name))


async def _set(name, value):
    pool = await _DEPS["get_db_pool"]()
    async with pool.acquire() as c:
        if value is None:
            await c.execute("DELETE FROM app_settings WHERE key=$1", _key(name))
        else:
            await c.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES ($1,$2,NOW()) "
                "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()",
                _key(name), value)


async def connected() -> bool:
    return configured() and bool(await _get("refresh_token"))


def _redirect_uri(request: Request) -> str:
    fixed = _env("GDRIVE_REDIRECT_URI")
    if fixed:
        return fixed
    proto = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    if "localhost" not in host and "127.0.0.1" not in host:
        proto = "https"
    return "%s://%s/admin/gdrive/callback" % (proto, host)


def _session():
    return _DEPS["http_session"]()


async def _access_token(session) -> str:
    rt = await _get("refresh_token")
    if not rt:
        raise RuntimeError("Google Drive nie jest połączony")
    cid, sec = _client()
    async with session.post(TOKEN_URL, data={
        "client_id": cid, "client_secret": sec,
        "refresh_token": rt, "grant_type": "refresh_token"}) as r:
        d = await r.json()
        if r.status != 200 or "access_token" not in d:
            if d.get("error") == "invalid_grant":
                await _set("refresh_token", None)
                raise RuntimeError("Dostęp do Google Drive wygasł lub został cofnięty — połącz ponownie")
            raise RuntimeError("Google token: %s" % (d.get("error_description") or d.get("error") or r.status))
        return d["access_token"]


def _h(tok):
    return {"Authorization": "Bearer " + tok}


async def _ensure_folder(session, tok) -> str:
    fid = await _get("folder_id")
    if fid:
        async with session.get("%s/files/%s" % (API, fid),
                               params={"fields": "id,trashed"}, headers=_h(tok)) as r:
            if r.status == 200 and not (await r.json()).get("trashed"):
                return fid
    name = "EKOMAT backups (%s)" % (_DEPS.get("instance_id") or "v2")
    async with session.post("%s/files" % API, params={"fields": "id"}, headers=_h(tok),
                            json={"name": name, "mimeType": FOLDER_MIME}) as r:
        d = await r.json()
        if r.status != 200:
            raise RuntimeError("Drive: nie udało się utworzyć folderu: %s" % d)
    await _set("folder_id", d["id"])
    return d["id"]


async def _list(session, tok, folder):
    files, page = [], None
    while True:
        params = {"q": "'%s' in parents and trashed=false" % folder,
                  "fields": "nextPageToken,files(id,name,size,createdTime,webViewLink)",
                  "pageSize": 200, "orderBy": "name desc"}
        if page:
            params["pageToken"] = page
        async with session.get("%s/files" % API, params=params, headers=_h(tok)) as r:
            d = await r.json()
            if r.status != 200:
                raise RuntimeError("Drive list: %s" % d)
        files += d.get("files", [])
        page = d.get("nextPageToken")
        if not page:
            return files


async def _cleanup(session, tok, folder) -> int:
    # имя вида ekomat-<инстанс>-YYYY-MM-DD_HH-MM.tar.gz
    files = []
    for f in await _list(session, tok, folder):
        m = DATE_RE.search(f["name"])
        if f["name"].startswith("ekomat-") and m:
            files.append((m.group(0), f))
    files.sort(key=lambda x: x[0])
    keep = {f["id"] for _, f in files[-_keep():]}
    seen_month = set()
    for stamp, f in files:                        # первая копия месяца — навсегда
        month = stamp[:7]
        if month not in seen_month:
            seen_month.add(month)
            keep.add(f["id"])
    removed = 0
    for _, f in files:
        if f["id"] in keep:
            continue
        async with session.patch("%s/files/%s" % (API, f["id"]), headers=_h(tok),
                                 json={"trashed": True}) as r:
            if r.status == 200:
                removed += 1
    return removed


async def upload(fname: str, blob: bytes) -> dict:
    """Залить архив в папку на Диске. Возвращает отчёт."""
    async with _session() as session:
        tok = await _access_token(session)
        folder = await _ensure_folder(session, tok)
        meta = {"name": fname, "parents": [folder]}
        async with session.post(UPLOAD, params={"uploadType": "resumable"},
                                headers={**_h(tok), "X-Upload-Content-Type": "application/gzip",
                                         "X-Upload-Content-Length": str(len(blob))},
                                json=meta) as r:
            if r.status != 200:
                raise RuntimeError("Drive upload start: %s" % await r.text())
            loc = r.headers["Location"]
        async with session.put(loc, data=blob, headers={"Content-Type": "application/gzip"}) as r:
            if r.status not in (200, 201):
                raise RuntimeError("Drive upload: %s" % await r.text())
            d = await r.json()
        removed = await _cleanup(session, tok, folder)
    await _set("last", json.dumps({"at": datetime.now(timezone.utc).isoformat(),
                                    "file": fname, "ok": True}))
    return {"ok": True, "file_id": d.get("id"), "removed_old_files": removed}


async def status() -> dict:
    out = {"configured": configured(), "connected": False, "files": [], "keep": _keep()}
    if not configured():
        return out
    out["connected"] = bool(await _get("refresh_token"))
    last = await _get("last")
    if last:
        try:
            out["last"] = json.loads(last)
        except ValueError:
            pass
    if out["connected"]:
        try:
            async with _session() as session:
                tok = await _access_token(session)
                folder = await _ensure_folder(session, tok)
                out["folder_url"] = "https://drive.google.com/drive/folders/" + folder
                out["files"] = [{"name": f["name"],
                                 "size_mb": round(int(f.get("size") or 0) / 1048576.0, 2),
                                 "url": f.get("webViewLink")}
                                for f in (await _list(session, tok, folder))[:60]]
        except Exception as e:
            out["error"] = str(e)
            out["connected"] = bool(await _get("refresh_token"))
    return out


# ==================================================================
# HTTP
# ==================================================================

async def _deny(request: Request):
    user = await get_current_user(request)
    if not user or not has_perm(user, "manage_users"):
        return JSONResponse({"error": "Tylko superadmin"}, status_code=403)
    return None


@gdrive_router.get("/admin/gdrive/status")
async def gdrive_status(request: Request):
    deny = await _deny(request)
    if deny:
        return deny
    return JSONResponse(await status())


@gdrive_router.get("/admin/gdrive/connect")
async def gdrive_connect(request: Request):
    deny = await _deny(request)
    if deny:
        return deny
    if not configured():
        return HTMLResponse("Brak GDRIVE_CLIENT_ID / GDRIVE_CLIENT_SECRET w ustawieniach serwera.",
                            status_code=400)
    state = secrets.token_urlsafe(24)
    await _set("oauth_state", state)
    params = {"client_id": _client()[0], "redirect_uri": _redirect_uri(request),
              "response_type": "code", "scope": SCOPE, "access_type": "offline",
              "prompt": "consent", "include_granted_scopes": "true", "state": state}
    return RedirectResponse(AUTH_URL + "?" + urlencode(params))


@gdrive_router.get("/admin/gdrive/callback")
async def gdrive_callback(request: Request):
    deny = await _deny(request)
    if deny:
        return deny
    q = request.query_params
    expected = await _get("oauth_state")
    await _set("oauth_state", None)
    if q.get("error"):
        return RedirectResponse("/admin/users?gdrive=denied#backup-section")
    if not expected or q.get("state") != expected or not q.get("code"):
        return HTMLResponse("Nieprawidłowy stan logowania — spróbuj ponownie.", status_code=400)
    cid, sec = _client()
    async with _session() as session:
        async with session.post(TOKEN_URL, data={
            "code": q["code"], "client_id": cid, "client_secret": sec,
            "redirect_uri": _redirect_uri(request), "grant_type": "authorization_code"}) as r:
            d = await r.json()
    if "refresh_token" not in d:
        return HTMLResponse("Google nie zwrócił refresh_token: %s" %
                            (d.get("error_description") or d.get("error") or "brak"), status_code=400)
    await _set("refresh_token", d["refresh_token"])
    await _set("folder_id", None)
    return RedirectResponse("/admin/users?gdrive=ok#backup-section")


@gdrive_router.post("/admin/gdrive/disconnect")
async def gdrive_disconnect(request: Request):
    deny = await _deny(request)
    if deny:
        return deny
    rt = await _get("refresh_token")
    if rt:
        try:
            async with _session() as session:
                await session.post("https://oauth2.googleapis.com/revoke", data={"token": rt})
        except Exception:
            pass
    await _set("refresh_token", None)
    return JSONResponse({"ok": True})
