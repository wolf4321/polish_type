#!/usr/bin/env bash
#
# clone_prod_db.sh — разовый клон боевой базы EKOMAT в базу v2.
#
# Прод затрагивается ТОЛЬКО на чтение: pg_dump не блокирует запись и не
# меняет данные. Люди продолжают работать, пока снимается дамп.
#
# Использование:
#   export PROD_DATABASE_URL='postgresql://...'   # боевая, только читаем
#   export V2_DATABASE_URL='postgresql://...'     # новая, сюда пишем
#   ./scripts/clone_prod_db.sh
#
# Требуется pg_dump/psql той же мажорной версии, что и сервер.

set -euo pipefail

RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[1;33m'; NC=$'\033[0m'
die() { echo "${RED}ОШИБКА:${NC} $*" >&2; exit 1; }
info() { echo "${GREEN}→${NC} $*"; }
warn() { echo "${YELLOW}!${NC} $*"; }

[[ -n "${PROD_DATABASE_URL:-}" ]] || die "не задан PROD_DATABASE_URL"
[[ -n "${V2_DATABASE_URL:-}" ]]   || die "не задан V2_DATABASE_URL"

command -v pg_dump >/dev/null || die "pg_dump не найден (поставь postgresql-client)"
command -v psql    >/dev/null || die "psql не найден (поставь postgresql-client)"

# ── Защита от самой дорогой ошибки ──────────────────────────
# Одинаковые строки подключения означали бы restore поверх боевой базы.
if [[ "$PROD_DATABASE_URL" == "$V2_DATABASE_URL" ]]; then
    die "PROD_DATABASE_URL и V2_DATABASE_URL совпадают. Это перезаписало бы прод."
fi

strip_creds() { sed -E 's#//[^@]*@#//#' <<<"$1"; }
PROD_HOSTDB=$(strip_creds "$PROD_DATABASE_URL")
V2_HOSTDB=$(strip_creds "$V2_DATABASE_URL")

if [[ "$PROD_HOSTDB" == "$V2_HOSTDB" ]]; then
    die "Хост и имя базы совпадают (${V2_HOSTDB}). Отличаются только учётные данные — это тот же сервер."
fi

STAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="${BACKUP_DIR:-./backups}"
DUMP_FILE="${BACKUP_DIR}/prod_${STAMP}.sql"
mkdir -p "$BACKUP_DIR"

echo
echo "  Источник (только чтение): $PROD_HOSTDB"
echo "  Приёмник (будет ЗАПОЛНЕН): $V2_HOSTDB"
echo "  Дамп:                      $DUMP_FILE"
echo

# ── Приёмник должен быть пуст ───────────────────────────────
info "Проверяю, что база v2 пуста..."
EXISTING=$(psql "$V2_DATABASE_URL" -tAc \
    "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'" 2>/dev/null) \
    || die "не удалось подключиться к базе v2"

if [[ "$EXISTING" -gt 0 ]]; then
    warn "В базе v2 уже есть таблиц: $EXISTING"
    warn "Restore поверх существующих данных приведёт к конфликтам и мусору."
    read -rp "Удалить схему public в базе v2 и продолжить? (напиши: yes) " ANSWER
    [[ "$ANSWER" == "yes" ]] || die "отменено"
    info "Пересоздаю схему public в базе v2..."
    psql "$V2_DATABASE_URL" -q -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
fi

# ── Снятие дампа ────────────────────────────────────────────
# --no-owner / --no-acl: на новом сервере другие роли, права прода не нужны.
info "Снимаю дамп с прода (запись на проде не блокируется)..."
pg_dump "$PROD_DATABASE_URL" \
    --no-owner --no-acl --format=plain --encoding=UTF8 \
    --file="$DUMP_FILE"

DUMP_SIZE=$(du -h "$DUMP_FILE" | cut -f1)
info "Дамп снят: $DUMP_FILE ($DUMP_SIZE)"

# ── Восстановление ──────────────────────────────────────────
info "Восстанавливаю в базу v2..."
psql "$V2_DATABASE_URL" -v ON_ERROR_STOP=0 -q -f "$DUMP_FILE"

# ── Вычистка боевых секретов из копии ───────────────────────
# Дамп принёс с собой app_settings, а там лежит боевой refresh_token
# Allegro. Токен одноразовый: одно обращение с копии — и прод теряет
# Allegro. Приложение читает ключи со своим префиксом (INSTANCE_ID),
# но боевая копия токена не должна лежать в базе разработки вообще.
info "Вычищаю боевые секреты из копии..."
WIPE_SQL="WITH gone AS (DELETE FROM app_settings WHERE key LIKE '%refresh_token%' OR key LIKE '%_secret%' OR key LIKE '%api_key%' RETURNING key) SELECT count(*) FROM gone"
WIPED=$(psql "$V2_DATABASE_URL" -tAc "$WIPE_SQL" 2>/dev/null || echo "?")
info "Удалено секретов из app_settings: $WIPED"

LEFT_SQL="SELECT count(*) FROM app_settings WHERE key LIKE '%token%' OR key LIKE '%secret%'"
LEFT=$(psql "$V2_DATABASE_URL" -tAc "$LEFT_SQL" 2>/dev/null || echo "?")
if [[ "$LEFT" != "0" && "$LEFT" != "?" ]]; then
    warn "Осталось подозрительных ключей: $LEFT. Проверь руками:"
    psql "$V2_DATABASE_URL" -tAc "SELECT '    ' || key FROM app_settings WHERE key LIKE '%token%' OR key LIKE '%secret%'"
fi

# ── Сверка ──────────────────────────────────────────────────
echo
info "Сверяю количество строк:"
printf '  %-16s %12s %12s\n' "таблица" "прод" "v2"
for TBL in orders users customers leads sync_log export_log app_settings; do
    P=$(psql "$PROD_DATABASE_URL" -tAc "SELECT count(*) FROM $TBL" 2>/dev/null || echo "-")
    V=$(psql "$V2_DATABASE_URL"   -tAc "SELECT count(*) FROM $TBL" 2>/dev/null || echo "-")
    MARK=""
    if [[ "$P" != "$V" ]]; then
        if [[ "$TBL" == "app_settings" ]]; then
            MARK="  <-- так и надо: вычистили секреты"
        else
            MARK="  <-- РАСХОДИТСЯ"
        fi
    fi
    printf '  %-16s %12s %12s%s\n' "$TBL" "$P" "$V" "$MARK"
done

echo
warn "Дамп содержит персональные данные клиентов. Не коммить его в гит"
warn "(.gitignore уже исключает *.sql и backups/) и удали, когда станет не нужен."
echo
info "Готово."
echo
echo "Дальше:"
echo "  1. В Railway у сервиса v2 задай SYNC_ENABLED=false и INSTANCE_ID=v2"
echo "  2. Переменные Allegro/Woo/Presta в v2 оставь ПУСТЫМИ"
echo "  3. Задеплой v2 — индексы создадутся при старте"
echo "  4. Войди теми же логином и паролем, что и на проде"
echo
echo "Четыре слоя защиты боевого токена Allegro:"
echo "  1. SYNC_ENABLED=false — планировщик не стартует, ручной синк отдаёт 503"
echo "  2. Барьер на сокете — любой исходящий запрос падает до соединения"
echo "  3. INSTANCE_ID=v2 — секреты читаются по своим ключам, боевые не видны"
echo "  4. Этот скрипт — боевой токен физически удалён из копии базы"
