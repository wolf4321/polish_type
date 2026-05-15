"""
auth.py — Permission-based auth for EKOMAT Dashboard.
Each user has a set of permissions (checkboxes).
Owner manages users and assigns permissions.
Sessions stored in DB (survive restarts).
"""

import os
import hashlib
import secrets
from datetime import datetime
from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="templates")
auth_router = APIRouter()

# ── All available permissions ──
ALL_PERMISSIONS = [
    ("dashboard",     "Dashboard",              "Widzi dashboard ze statystykami"),
    ("orders",        "Zamówienia",             "Widzi tabelę zamówień"),
    ("prices",        "Ceny i przychody",       "Widzi ceny, koszty dostawy, przychody"),
    ("phones",        "Telefony klientów",      "Widzi numery telefonów"),
    ("export",        "Eksport XLSX",           "Może eksportować zamówienia"),
    ("sync",          "Synchronizacja",         "Może uruchamiać sync"),
    ("add_orders",    "Dodawanie zamówień",     "Może dodawać zamówienia ręcznie"),
    ("edit_comments", "Edycja komentarzy",      "Może edytować komentarze"),
    ("mark_delivery", "Oznaczanie dostawy",     "Może oznaczać zamówienia jako dostarczone"),
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
def _hash_pw(password: str) -> str:
    salt = "ekomat-salt-2024"
    return hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()


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

    # Bootstrap: create owner if no users exist
    count = await conn.fetchval("SELECT COUNT(*) FROM users")
    if count == 0:
        owner_email = os.getenv("OWNER_EMAIL", "admin@ekomat.pl")
        owner_pw = os.getenv("OWNER_PASSWORD", "admin")
        owner_name = os.getenv("OWNER_NAME", "Admin")
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
    pool = await _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO user_sessions (token, user_id) VALUES ($1, $2)",
            token, user_id,
        )
        await conn.execute(
            "UPDATE users SET last_login=$1 WHERE id=$2",
            datetime.now(), user_id,
        )
    return token


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
            """, token)
        except Exception:
            row = await conn.fetchrow("""
                SELECT u.id, u.email, u.name, u.role, u.is_active
                FROM user_sessions s
                JOIN users u ON u.id = s.user_id
                WHERE s.token = $1 AND u.is_active = TRUE
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
    if not row or row["pw_hash"] != _hash_pw(password):
        return templates.TemplateResponse(
            request=request, name="login.html",
            context={"error": "Nieprawidłowy email lub hasło"},
        )
    if not row["is_active"]:
        return templates.TemplateResponse(
            request=request, name="login.html",
            context={"error": "Konto jest nieaktywne. Skontaktuj się z właścicielem."},
        )
    token = await _create_session(row["id"])
    resp = RedirectResponse("/", status_code=302)
    resp.set_cookie("session_token", token, httponly=True, max_age=86400 * 30)
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
            "SELECT id, email, name, role, permissions, is_active, created_at, last_login FROM users ORDER BY id"
        )
    users = []
    for r in rows:
        users.append({
            "id": r["id"],
            "email": r["email"],
            "name": r["name"],
            "role": r.get("role", ""),
            "permissions": r.get("permissions", ""),
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
            """INSERT INTO users (email, name, pw_hash, role, permissions, invited_by)
               VALUES ($1, $2, $3, $4, $5, $6) RETURNING id""",
            email, name, _hash_pw(password), role, permissions, owner["id"],
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

        role = "owner" if "manage_users" in permissions else "user"

        await conn.execute(
            "UPDATE users SET name=$1, role=$2, is_active=$3, permissions=$4 WHERE id=$5",
            name, role, is_active, permissions, user_id,
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
        if current_hash != _hash_pw(old_pw):
            raise HTTPException(400, "Wrong current password")
        await conn.execute(
            "UPDATE users SET pw_hash=$1 WHERE id=$2",
            _hash_pw(new_pw), user["id"],
        )
    return JSONResponse({"ok": True})
