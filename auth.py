"""
auth.py — Permission-based auth for EKOMAT Dashboard.
Each user has a set of permissions (checkboxes).
Owner manages users and assigns permissions.
Sessions stored in DB (survive restarts).
"""

import os
import hashlib
import hmac
import secrets
from datetime import datetime, timedelta
from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

try:
    import bcrypt
    BCRYPT_AVAILABLE = True
except ImportError:  # pragma: no cover
    BCRYPT_AVAILABLE = False
    print("[auth] bcrypt не установлен — пароли останутся на старом хеше. "
          "Добавь bcrypt в requirements.txt")

templates = Jinja2Templates(directory="templates")
auth_router = APIRouter()

# ── Настройки сессий и cookie ──
# Срок жизни сессии. Раньше колонка expires_at существовала, но никогда
# не заполнялась и не проверялась — сессия жила вечно.
SESSION_TTL_DAYS = int(os.getenv("SESSION_TTL_DAYS", "30"))

# secure=True требует HTTPS. Railway отдаёт HTTPS, поэтому по умолчанию включено.
# Для локальной разработки по http:// поставь COOKIE_SECURE=false.
COOKIE_SECURE = (os.getenv("COOKIE_SECURE", "true").strip().lower()
                 in ("1", "true", "yes", "on"))
COOKIE_SAMESITE = os.getenv("COOKIE_SAMESITE", "lax")

# ── All available permissions ──
ALL_PERMISSIONS = [
    ("dashboard",     "Dashboard",              "Widzi dashboard ze statystykami"),
    ("orders",        "Zamówienia",             "Widzi tabelę zamówień"),
    ("view_stats",    "Statystyki (karty)",     "Widzi karty statystyk na stronie zamówień"),
    ("prices",        "Ceny i przychody",       "Widzi ceny, koszty dostawy, przychody"),
    ("phones",        "Telefony klientów",      "Widzi numery telefonów"),
    ("export",        "Eksport XLSX",           "Może eksportować zamówienia"),
    ("sync",          "Synchronizacja",         "Może uruchamiać sync"),
    ("add_orders",    "Dodawanie zamówień",     "Może dodawać zamówienia ręcznie"),
    ("edit_comments", "Edycja komentarzy",      "Może edytować komentarze"),
    ("mark_delivery", "Oznaczanie dostawy",     "Może oznaczać zamówienia jako dostarczone"),
    ("products",      "Katalog produktów",      "Może edytować katalog: nazwy, kody EAN, wagę"),
    ("assistant_kb",  "Baza wiedzy asystenta",  "Może edytować odpowiedzi asystenta i widzi pytania bez odpowiedzi"),
    ("manage_users",  "Zarządzanie użytkownikami", "Może dodawać/edytować/usuwać użytkowników"),
]

OWNER_PERMS = ",".join(p[0] for p in ALL_PERMISSIONS)

# ── DB pool (shared with main.py — injected at startup) ──
_pool = None

def set_db_pool(pool):
    global _pool
    _pool = pool

async def _get_pool():
    if _pool is None:
        raise RuntimeError("DB pool not initialized in auth module")
    return _pool


# ── Password hashing ──
#
# Старая схема: SHA-256 с одной солью на всех пользователей. SHA-256 считается
# миллиардами хешей в секунду на обычной видеокарте, а общая соль означает,
# что одинаковые пароли дают одинаковый хеш — подбор по словарю ломает всю
# базу разом.
#
# Новая схема: bcrypt, отдельная соль на пароль, настраиваемая стоимость.
# Миграция прозрачная: старый хеш продолжает приниматься при входе, и в тот же
# момент пароль перезаписывается bcrypt-хешем. Никто ничего не замечает,
# сбрасывать пароли не нужно.

_LEGACY_SALT = "ekomat-salt-2024"
BCRYPT_ROUNDS = int(os.getenv("BCRYPT_ROUNDS", "12"))

# Переезд со старой версии: пока BCRYPT_MIGRATE не включён, пароли остаются
# в старом формате (и новые тоже пишутся в нём). Так откат на старый код
# не запрёт людей — старый код bcrypt не понимает. Входить можно с любым
# форматом. Когда откат больше не нужен — BCRYPT_MIGRATE=true.
BCRYPT_MIGRATE = (os.getenv("BCRYPT_MIGRATE") or "").strip().lower() in ("1", "true", "yes", "on")


def _hash_pw_legacy(password: str) -> str:
    """Старый SHA-256-хеш. Только для проверки унаследованных паролей."""
    return hashlib.sha256(f"{_LEGACY_SALT}:{password}".encode()).hexdigest()


def _is_legacy_hash(stored: str) -> bool:
    """Старый хеш — 64 hex-символа. bcrypt начинается с $2."""
    return bool(stored) and not stored.startswith("$2") and len(stored) == 64


def _hash_pw(password: str) -> str:
    """Хеширует пароль для сохранения. bcrypt, если доступен."""
    if BCRYPT_AVAILABLE and BCRYPT_MIGRATE:
        return bcrypt.hashpw(password.encode("utf-8"),
                             bcrypt.gensalt(rounds=BCRYPT_ROUNDS)).decode("utf-8")
    return _hash_pw_legacy(password)


def _verify_pw(password: str, stored: str) -> bool:
    """Проверяет пароль против сохранённого хеша любого из двух форматов."""
    if not stored:
        return False
    if _is_legacy_hash(stored):
        # compare_digest — чтобы время сравнения не зависело от совпадения.
        return hmac.compare_digest(_hash_pw_legacy(password), stored)
    if not BCRYPT_AVAILABLE:
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"), stored.encode("utf-8"))
    except (ValueError, TypeError):
        return False


async def _upgrade_hash_if_legacy(conn, user_id: int, password: str, stored: str):
    """После успешного входа по старому хешу тихо переводит пароль на bcrypt."""
    if not BCRYPT_AVAILABLE or not BCRYPT_MIGRATE or not _is_legacy_hash(stored):
        return
    try:
        await conn.execute("UPDATE users SET pw_hash=$1 WHERE id=$2",
                           _hash_pw(password), user_id)
        print(f"[auth] пароль пользователя #{user_id} переведён на bcrypt")
    except Exception as e:
        # Не критично: вход уже состоялся, переведём при следующем.
        print(f"[auth] не удалось обновить хеш для #{user_id}: {e}")


# ── Permission helpers ──
def parse_perms(perms_str: str) -> set:
    """Parse comma-separated permissions string into a set."""
    if not perms_str:
        return set()
    return {p.strip() for p in perms_str.split(",") if p.strip()}


def has_perm(user: dict, perm: str) -> bool:
    """Check if user has a specific permission."""
    if not user:
        return False
    perms = parse_perms(user.get("permissions", ""))
    return perm in perms


# ============================================================
# DB Init — create tables for users & sessions
# ============================================================

async def init_auth_tables(conn):
    """Create users + sessions tables. Called from main.py lifespan."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id           SERIAL PRIMARY KEY,
            email        VARCHAR(200) UNIQUE NOT NULL,
            name         VARCHAR(200) NOT NULL,
            pw_hash      VARCHAR(200),
            role         VARCHAR(20) NOT NULL DEFAULT 'viewer',
            permissions  TEXT DEFAULT '',
            allowed_sources TEXT DEFAULT 'all',
            is_active    BOOLEAN DEFAULT TRUE,
            invited_by   INTEGER REFERENCES users(id),
            created_at   TIMESTAMP DEFAULT NOW(),
            last_login   TIMESTAMP
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            token       VARCHAR(100) PRIMARY KEY,
            user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
            created_at  TIMESTAMP DEFAULT NOW(),
            expires_at  TIMESTAMP
        )
    """)

    # Migrate: add columns that may not exist yet (for existing DBs)
    for col_migration in [
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS permissions TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS allowed_sources TEXT DEFAULT 'all'",
    ]:
        try:
            await conn.execute(col_migration)
        except Exception:
            pass  # column already exists or other benign error

    # Сессии, созданные до появления TTL, имеют expires_at = NULL и живут вечно.
    # Проставляем им срок от текущего момента: никого не выкидываем из системы,
    # но дальше они истекают как положено.
    try:
        updated = await conn.execute(
            "UPDATE user_sessions SET expires_at = NOW() + ($1 || ' days')::interval "
            "WHERE expires_at IS NULL",
            str(SESSION_TTL_DAYS),
        )
        if updated and updated != "UPDATE 0":
            print(f"[auth] бессрочным сессиям проставлен TTL: {updated}")
    except Exception as e:
        print(f"[auth] не удалось проставить TTL сессиям: {e}")

    # Bootstrap: create owner if no users exist
    count = await conn.fetchval("SELECT COUNT(*) FROM users")
    if count == 0:
        owner_email = os.getenv("OWNER_EMAIL", "admin@ekomat.pl")
        owner_pw = os.getenv("OWNER_PASSWORD", "")
        owner_name = os.getenv("OWNER_NAME", "Admin")
        if not owner_pw:
            # Раньше здесь по умолчанию стоял пароль "admin". Если переменную
            # забыли задать, панель поднималась с общеизвестным доступом.
            owner_pw = secrets.token_urlsafe(16)
            print("[auth] " + "!" * 60)
            print("[auth] OWNER_PASSWORD не задан. Сгенерирован разовый пароль:")
            print(f"[auth]     {owner_email} / {owner_pw}")
            print("[auth] Войди и смени его. Пароль больше нигде не сохранён.")
            print("[auth] " + "!" * 60)
        await conn.execute(
            """INSERT INTO users (email, name, pw_hash, role, permissions)
               VALUES ($1, $2, $3, 'owner', $4)""",
            owner_email, owner_name, _hash_pw(owner_pw), OWNER_PERMS,
        )
        print(f"[auth] Bootstrap owner created: {owner_email}")
    else:
        # Ensure existing owners have all permissions
        await conn.execute(
            "UPDATE users SET permissions=$1 WHERE role='owner'",
            OWNER_PERMS,
        )


# ============================================================
# Session helpers
# ============================================================

async def _create_session(user_id: int) -> str:
    token = secrets.token_hex(32)
    expires_at = datetime.now() + timedelta(days=SESSION_TTL_DAYS)
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO user_sessions (token, user_id, expires_at) VALUES ($1, $2, $3)",
            token, user_id, expires_at,
        )
        await conn.execute(
            "UPDATE users SET last_login=$1 WHERE id=$2",
            datetime.now(), user_id,
        )
        # Попутная уборка: протухшие сессии копятся вечно и ничего не дают.
        try:
            await conn.execute(
                "DELETE FROM user_sessions WHERE expires_at IS NOT NULL AND expires_at < NOW()"
            )
        except Exception:
            pass
    return token


def _set_session_cookie(resp, token: str):
    """Ставит cookie сессии. secure + samesite добавлены в v2."""
    resp.set_cookie(
        "session_token",
        token,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
        max_age=86400 * SESSION_TTL_DAYS,
        path="/",
    )


async def get_current_user(request: Request) -> dict | None:
    """Get current user from session cookie."""
    token = request.cookies.get("session_token")
    if not token:
        return None
    pool = await _get_pool()
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow("""
                SELECT u.id, u.email, u.name, u.role, u.is_active,
                       u.permissions, u.allowed_sources
                FROM user_sessions s
                JOIN users u ON u.id = s.user_id
                WHERE s.token = $1 AND u.is_active = TRUE
                  AND (s.expires_at IS NULL OR s.expires_at > NOW())
            """, token)
        except Exception:
            row = await conn.fetchrow("""
                SELECT u.id, u.email, u.name, u.role, u.is_active
                FROM user_sessions s
                JOIN users u ON u.id = s.user_id
                WHERE s.token = $1 AND u.is_active = TRUE
                  AND (s.expires_at IS NULL OR s.expires_at > NOW())
            """, token)
    if not row:
        return None
    d = dict(row)
    if "permissions" not in d:
        d["permissions"] = OWNER_PERMS if d.get("role") == "owner" else ""
    if "allowed_sources" not in d:
        d["allowed_sources"] = "all"
    return d


async def require_user(request: Request) -> dict:
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


# ============================================================
# Login / Logout
# ============================================================

@auth_router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    user = await get_current_user(request)
    if user:
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse(request=request, name="login.html", context={"error": None})


@auth_router.post("/login")
async def login_post(request: Request, email: str = Form(...), password: str = Form(...)):
    email = email.strip().lower()
    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, pw_hash, is_active FROM users WHERE email=$1",
            email,
        )
        if not row or not _verify_pw(password, row["pw_hash"]):
            return templates.TemplateResponse(
                request=request, name="login.html",
                context={"error": "Nieprawidłowy email lub hasło"},
            )
        if not row["is_active"]:
            return templates.TemplateResponse(
                request=request, name="login.html",
                context={"error": "Konto jest nieaktywne. Skontaktuj się z właścicielem."},
            )
        # Пароль верный. Если он ещё на старом SHA-256 — переводим на bcrypt.
        await _upgrade_hash_if_legacy(conn, row["id"], password, row["pw_hash"])

    token = await _create_session(row["id"])
    resp = RedirectResponse("/", status_code=302)
    _set_session_cookie(resp, token)
    return resp


@auth_router.get("/logout")
async def logout(request: Request):
    token = request.cookies.get("session_token")
    if token:
        pool = await _get_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM user_sessions WHERE token=$1", token)
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("session_token")
    return resp


# ============================================================
# User Management API (manage_users permission required)
# ============================================================

@auth_router.get("/admin/users", response_class=HTMLResponse)
async def admin_users_page(request: Request):
    user = await get_current_user(request)
    if not user or not has_perm(user, "manage_users"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse(
        request=request, name="admin_users.html",
        context={"user": user, "all_permissions": ALL_PERMISSIONS},
    )


@auth_router.get("/api/users")
async def api_list_users(request: Request):
    user = await get_current_user(request)
    if not user or not has_perm(user, "manage_users"):
        raise HTTPException(403, "Access denied")
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, email, name, role, permissions, allowed_sources, is_active, created_at, last_login FROM users ORDER BY id"
        )
    users = []
    for r in rows:
        users.append({
            "id": r["id"],
            "email": r["email"],
            "name": r["name"],
            "role": r.get("role", ""),
            "permissions": r.get("permissions", ""),
            "allowed_sources": r.get("allowed_sources", "all"),
            "is_active": r["is_active"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "last_login": r["last_login"].isoformat() if r["last_login"] else None,
        })
    return JSONResponse({"users": users})


@auth_router.post("/api/users")
async def api_create_user(request: Request):
    """Create a new user with specific permissions."""
    owner = await get_current_user(request)
    if not owner or not has_perm(owner, "manage_users"):
        raise HTTPException(403, "Access denied")

    data = await request.json()
    email = (data.get("email") or "").strip().lower()
    name = data.get("name", "").strip()
    password = data.get("password", "")
    permissions = data.get("permissions", "")
    allowed_sources = data.get("allowed_sources", "all")

    if not email or not name:
        raise HTTPException(400, "Email and name are required")
    if not password or len(password) < 4:
        raise HTTPException(400, "Password must be at least 4 characters")

    # Determine role from permissions
    role = "owner" if "manage_users" in permissions else "user"

    pool = await _get_pool()
    async with pool.acquire() as conn:
        exists = await conn.fetchval("SELECT id FROM users WHERE email=$1", email)
        if exists:
            raise HTTPException(409, "User with this email already exists")
        user_id = await conn.fetchval(
            """INSERT INTO users (email, name, pw_hash, role, permissions, allowed_sources, invited_by)
               VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id""",
            email, name, _hash_pw(password), role, permissions, allowed_sources, owner["id"],
        )
    return JSONResponse({"ok": True, "id": user_id})


@auth_router.put("/api/users/{user_id}")
async def api_update_user(user_id: int, request: Request):
    """Update user permissions, name, active status."""
    caller = await get_current_user(request)
    if not caller or not has_perm(caller, "manage_users"):
        raise HTTPException(403, "Access denied")
    if user_id == caller["id"]:
        raise HTTPException(400, "Cannot edit your own account here")

    data = await request.json()
    pool = await _get_pool()
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE id=$1", user_id)
        if not user:
            raise HTTPException(404, "User not found")

        name = data.get("name", user["name"]).strip()
        is_active = data.get("is_active", user["is_active"])
        permissions = data.get("permissions", user.get("permissions", ""))
        allowed_sources = data.get("allowed_sources", user.get("allowed_sources", "all"))

        role = "owner" if "manage_users" in permissions else "user"

        await conn.execute(
            "UPDATE users SET name=$1, role=$2, is_active=$3, permissions=$4, allowed_sources=$5 WHERE id=$6",
            name, role, is_active, permissions, allowed_sources, user_id,
        )

        # If password provided — update it
        new_pw = data.get("password", "").strip()
        if new_pw and len(new_pw) >= 4:
            await conn.execute(
                "UPDATE users SET pw_hash=$1 WHERE id=$2",
                _hash_pw(new_pw), user_id,
            )

        # If deactivated — kill sessions
        if not is_active:
            await conn.execute("DELETE FROM user_sessions WHERE user_id=$1", user_id)

    return JSONResponse({"ok": True})


@auth_router.delete("/api/users/{user_id}")
async def api_delete_user(user_id: int, request: Request):
    caller = await get_current_user(request)
    if not caller or not has_perm(caller, "manage_users"):
        raise HTTPException(403, "Access denied")
    if user_id == caller["id"]:
        raise HTTPException(400, "Cannot delete your own account")
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM user_sessions WHERE user_id=$1", user_id)
        await conn.execute("DELETE FROM users WHERE id=$1", user_id)
    return JSONResponse({"ok": True})


# ============================================================
# Change own password
# ============================================================

@auth_router.post("/api/me/password")
async def api_change_password(request: Request):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(401, "Not authenticated")
    data = await request.json()
    old_pw = data.get("old_password", "")
    new_pw = data.get("new_password", "")
    if not new_pw or len(new_pw) < 4:
        raise HTTPException(400, "New password must be at least 4 characters")
    pool = await _get_pool()
    async with pool.acquire() as conn:
        current_hash = await conn.fetchval("SELECT pw_hash FROM users WHERE id=$1", user["id"])
        if not _verify_pw(old_pw, current_hash):
            raise HTTPException(400, "Wrong current password")
        await conn.execute(
            "UPDATE users SET pw_hash=$1 WHERE id=$2",
            _hash_pw(new_pw), user["id"],
        )
        # Пароль сменился — все прочие сессии этого пользователя закрываем.
        # Раньше старая сессия с украденной cookie пережила бы смену пароля.
        current_token = request.cookies.get("session_token")
        await conn.execute(
            "DELETE FROM user_sessions WHERE user_id=$1 AND token <> $2",
            user["id"], current_token or "",
        )
    return JSONResponse({"ok": True})
