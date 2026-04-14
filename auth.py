"""
auth.py — Простая cookie-авторизация.
Логин через /login, хеш пароля в env var AUTH_HASH.
"""

import os
import hashlib
import secrets
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="templates")
auth_router = APIRouter()

# Сесії (in-memory, при рестарті скидаються)
_sessions = {}

def _hash(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

async def get_current_user(request: Request) -> dict | None:
    token = request.cookies.get("session_token")
    if token and token in _sessions:
        return _sessions[token]
    return None

@auth_router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse(request=request, name="login.html", context={})

@auth_router.post("/login")
async def login_post(request: Request, password: str = Form(...)):
    expected = os.getenv("AUTH_HASH", _hash("admin"))  # дефолт: пароль "admin"
    if _hash(password) == expected:
        token = secrets.token_hex(32)
        _sessions[token] = {"role": "admin"}
        resp = RedirectResponse("/", status_code=302)
        resp.set_cookie("session_token", token, httponly=True, max_age=86400*30)
        return resp
    return templates.TemplateResponse(
        request=request, name="login.html",
        context={"error": "Невірний пароль"}
    )

@auth_router.get("/logout")
async def logout(request: Request):
    token = request.cookies.get("session_token")
    if token in _sessions:
        del _sessions[token]
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("session_token")
    return resp
