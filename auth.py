"""
auth.py — Role-based auth for EKOMAT Dashboard.
Roles: owner, manager, logistics, viewer.
Login via email + password.  Users managed by owner only.
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

# ── Roles ──
ROLES = ("owner", "manager", "logistics", "viewer")
ROLE_LABELS = {
    "owner":     "Właściciel",
    "manager":   "Menedżer",
    "logistics": "Logistyka",
    "viewer":    "Podgląd",
}

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

    # Bootstrap: create owner if no users exist
    count = await conn.fetchval("SELECT COUNT(*) FROM users")
    if count == 0:
        owner_email = os.getenv("OWNER_EMAIL", "admin@ekomat.pl")
        owner_pw = os.getenv("OWNER_PASSWORD", "admin")
        owner_name = os.getenv("OWNER_NAME", "Admin")
        await conn.execute(
            """INSERT INTO users (email, name, pw_hash, role)
               VALUES ($1, $2, $3, 'owner')""",
            owner_email, owner_name, _hash_pw(owner_pw),
        )
        print(f"[auth] Bootstrap owner created: {owner_email}")


# ============================================================
# Session helpers
# ============================================================

async def _create_session(user_id: int) -> str:
    """Create a session token for user, store in DB."""
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
    """Get current user from session cookie. Returns dict with id, email, name, role."""
    token = request.cookies.get("session_token")
    if not token:
        return None
    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT u.id, u.email, u.name, u.role, u.is_active, u.allowed_sources
            FROM user_sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.token = $1 AND u.is_active = TRUE
        """, token)
    if not row:
        return None
    return dict(row)


async def require_user(request: Request) -> dict:
    """Require authenticated user. Redirects to /login if not found."""
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


async def require_role(request: Request, *roles) -> dict:
    """Require user with specific role(s)."""
    user = await require_user(request)
    if user["role"] not in roles:
        raise HTTPException(status_code=403, detail="Access denied")
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
            "SELECT id, pw_hash, role, is_active FROM users WHERE email=$1",
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
# User Management API (owner only)
# ============================================================

@auth_router.get("/admin/users", response_class=HTMLResponse)
async def admin_users_page(request: Request):
    user = await get_current_user(request)
    if not user or user["role"] != "owner":
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse(
        request=request, name="admin_users.html",
        context={"user": user, "roles": ROLES, "role_labels": ROLE_LABELS},
    )


@auth_router.get("/api/users")
async def api_list_users(request: Request):
    user = await get_current_user(request)
    if not user or user["role"] != "owner":
        raise HTTPException(403, "Only owner can manage users")
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, email, name, role, allowed_sources, is_active, created_at, last_login FROM users ORDER BY id"
        )
    users = []
    for r in rows:
        users.append({
            "id": r["id"],
            "email": r["email"],
            "name": r["name"],
            "role": r["role"],
            "allowed_sources": r.get("allowed_sources", "all"),
            "is_active": r["is_active"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "last_login": r["last_login"].isoformat() if r["last_login"] else None,
        })
    return JSONResponse({"users": users})


@auth_router.post("/api/users")
async def api_create_user(request: Request):
    """Owner creates a new user."""
    owner = await get_current_user(request)
    if not owner or owner["role"] != "owner":
        raise HTTPException(403, "Only owner can add users")

    data = await request.json()
    email = (data.get("email") or "").strip().lower()
    name = data.get("name", "").strip()
    role = data.get("role", "viewer")
    password = data.get("password", "")
    allowed_sources = data.get("allowed_sources", "all")

    if not email or not name:
        raise HTTPException(400, "Email and name are required")
    if role not in ROLES:
        raise HTTPException(400, f"Invalid role: {role}")
    if not password or len(password) < 4:
        raise HTTPException(400, "Password must be at least 4 characters")

    pool = await _get_pool()
    async with pool.acquire() as conn:
        exists = await conn.fetchval("SELECT id FROM users WHERE email=$1", email)
        if exists:
            raise HTTPException(409, "User with this email already exists")
        user_id = await conn.fetchval(
            """INSERT INTO users (email, name, pw_hash, role, allowed_sources, invited_by)
               VALUES ($1, $2, $3, $4, $5, $6) RETURNING id""",
            email, name, _hash_pw(password), role, allowed_sources, owner["id"],
        )
    return JSONResponse({"ok": True, "id": user_id})


@auth_router.put("/api/users/{user_id}")
async def api_update_user(user_id: int, request: Request):
    """Owner updates user role, name, active status."""
    owner = await get_current_user(request)
    if not owner or owner["role"] != "owner":
        raise HTTPException(403, "Only owner can edit users")
    if user_id == owner["id"]:
        raise HTTPException(400, "Cannot edit your own account here")

    data = await request.json()
    pool = await _get_pool()
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE id=$1", user_id)
        if not user:
            raise HTTPException(404, "User not found")

        name = data.get("name", user["name"]).strip()
        role = data.get("role", user["role"])
        is_active = data.get("is_active", user["is_active"])
        allowed_sources = data.get("allowed_sources", user.get("allowed_sources", "all"))

        if role not in ROLES:
            raise HTTPException(400, f"Invalid role: {role}")

        await conn.execute(
            "UPDATE users SET name=$1, role=$2, is_active=$3, allowed_sources=$4 WHERE id=$5",
            name, role, is_active, allowed_sources, user_id,
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
    """Owner deletes a user."""
    owner = await get_current_user(request)
    if not owner or owner["role"] != "owner":
        raise HTTPException(403, "Only owner can delete users")
    if user_id == owner["id"]:
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
    """Any user can change their own password."""
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
