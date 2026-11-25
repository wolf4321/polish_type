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

## Каталог товаров (продуктизация)

- `catalog.py` — таблицы `products` / `product_codes`, API `/api/products`, страница `/products` (право `products`).
- Сид: `data/products_seed.json`, собирается из Excel клиента `python3 scripts/build_products_seed.py`.
  Заливается автоматически при старте, только если таблица `products` пустая.
- Один товар — много кодов (EAN OTEKO, EAN EKOMAT, Allegro). Код, который стоит у двух товаров,
  в автоподборе не участвует — менеджер выбирает товар сам.
- Ручной заказ: позиции из каталога (кнопки: категория → параметры) или вручную; цена вводится руками;
  вес = вес/шт × кол-во; доставка одна на заказ. Вес заказа — колонка `orders.weight_kg`, в таблице и в XLSX («Waga (kg)»).
- Заказы из синка: если код позиции (sku/ean) есть в каталоге — название и вес берутся из каталога,
  исходное название остаётся в `orig_name`.

## Помощник (без внешней модели)

- `assistant.py` + `static/assistant.js` (окно «💬 Asystent» на всех страницах) + `/assistant` — база знаний (право `assistant_kb`).
- Разбор вопроса PL/RU: намерение по ключевым словам (`INTENTS`) + сущности (магазин, период, номер, телефон, имя, товар) → ответ из базы.
  Вопросы «как/что значит» и всё непонятое → база знаний `kb_qa` (pg_trgm). Не знает — кнопка «Научить», ответ сохраняется в базу.
- Действия (приоритет, комментарий) — только с подтверждением, пишутся в `action_log`.
- Лог `assistant_log`: 👍/👎, вопросы без ответа видны на `/assistant` — по ним добавлять шаблоны.
- Новое намерение = строка в `INTENTS` + ветка в `answer()`. Позже сюда же можно подключить LLM: намерения — это готовые инструменты.

## Бэкап на Google Диск

Каждая ночная копия базы (тот же архив, что уходит на GitHub) заливается ещё и в папку `EKOMAT backups (<инстанс>)` на Google Диске.

1. Google Cloud → APIs & Services: включить **Google Drive API**, создать OAuth-клиент **Web application**, redirect URI: `https://<домен панели>/admin/gdrive/callback`.
2. Railway: `GDRIVE_CLIENT_ID`, `GDRIVE_CLIENT_SECRET` (необязательно: `GDRIVE_REDIRECT_URI`, `GDRIVE_KEEP`, по умолчанию 30).
3. Панель → Użytkownicy → «Kopie zapasowe» → «Połącz Google Drive».

Доступ `drive.file`: приложение видит только свои файлы. Хранятся последние `GDRIVE_KEEP` копий плюс первая копия каждого месяца; лишние уходят в корзину Диска. Токен лежит в `app_settings` и в архивы не попадает.
