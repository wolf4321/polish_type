# Dashboard Boilerplate

Универсальная коробка для аналитических проектов на FastAPI + asyncpg + Jinja2.
Вытянута из проекта AutoGroup Analytics.

## Что внутри

```
main.py          — FastAPI app, webhook-приём, API, фоновые задачи
auth.py          — Cookie-авторизация (простая, без внешних зависимостей)
templates/       — Jinja2 шаблоны (index + login)
static/          — CSS/JS (пока пустая, добавляй свои)
requirements.txt — Зависимости
Procfile         — Для Railway/Heroku деплоя
```

## Быстрый старт

### 1. Локально
```bash
pip install -r requirements.txt
export DB_CONNECT="postgresql://user:pass@localhost:5432/mydb"
uvicorn main:app --reload
```

### 2. Railway
1. Создай проект на railway.app
2. Добавь PostgreSQL плагин
3. Задай env vars:
   - `DB_CONNECT` — строка подключения к PostgreSQL
   - `AUTH_HASH` — sha256 хеш пароля (python3 -c "import hashlib; print(hashlib.sha256(b'mypassword').hexdigest())")
4. Деплой через GitHub или `railway up`

## Webhooks

Форма/лід:
```
POST /api/webhook/lead
Body: { "name": "...", "phone": "...", "utm_source": "...", "gclid": "..." }
```

Дзвінок:
```
POST /api/webhook/call
Body: { "phone": "...", "duration": 45, "status": "answered" }
```

## Как адаптировать под свой проект

### Bitrix24
1. Раскомментируй `send_to_bitrix()` и `sync_from_bitrix()` в main.py
2. Добавь env var `BITRIX_WEBHOOK_URL`
3. Добавь колонку: `ALTER TABLE leads ADD COLUMN bitrix_id INTEGER`

### Allegro
1. Добавь `ALLEGRO_API_KEY` в env vars
2. Создай таблицу orders и API-эндпоинты
3. Используй scheduler для периодического синка

### Google Ads
1. Раскомментируй `google-ads` в requirements.txt
2. Добавь env vars: GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID, etc.
3. Добавь функцию enrich_lead_by_gclid() (см. AutoGroup проект)

### Binotel / Call Tracking
1. Настрой вебхук Binotel на `/api/webhook/call`
2. Расширь парсинг данных в `webhook_call()`

## Архитектура

```
Браузер ←→ FastAPI (main.py)
                ├── asyncpg ←→ PostgreSQL
                ├── Jinja2 → HTML шаблоны
                ├── APScheduler → фоновые задачи
                └── aiohttp → внешние API (Bitrix, Google, etc.)
```

## Env Vars

| Переменная | Описание | Обязательная |
|---|---|---|
| DB_CONNECT | PostgreSQL connection string | ✅ |
| AUTH_HASH | SHA256 хеш пароля | Нет (дефолт: "admin") |
| BITRIX_WEBHOOK_URL | Bitrix24 REST webhook URL | Нет |
| ALLEGRO_API_KEY | Allegro API ключ | Нет |
