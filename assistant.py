"""
Помощник EKOMAT («мозги» системы) — без внешней языковой модели.

Как отвечает:
  1. Разбирает вопрос (PL/RU): ищет намерение по ключевым словам и сущности —
     магазин, период, номер заказа, телефон, имя, товар из каталога.
  2. Если намерение известно — отвечает живыми данными из базы (заказы, каталог, синк).
  3. Если нет — ищет похожий вопрос в базе знаний (kb_qa, триграммы pg_trgm).
  4. Если и там нет — честно говорит «не знаю» и предлагает научить: пользователь
     пишет правильный ответ, и он сохраняется в базу знаний.
  5. Простые действия (приоритет, комментарий) — только после подтверждения,
     с записью в общий журнал изменений.

Обучение: 👍/👎 к ответам (assistant_log.feedback, kb_qa.helpful), ответы сотрудников
в базу знаний, лог вопросов без ответа — по нему видно, какие шаблоны добавить.

Языковую модель можно подключить позже: всё, что делают намерения, — это вызовы
инструментов с параметрами; модель будет выбирать их сама вместо ключевых слов.
"""
import json
import re
import secrets
import time
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_user, has_perm

router = APIRouter()
templates = Jinja2Templates(directory="templates")
TZ = ZoneInfo("Europe/Warsaw")

_ctx = {}  # get_pool, log_action, order_label, status_pl — из main.configure


def configure(**kw):
    _ctx.update(kw)


async def _pool():
    return await _ctx["get_pool"]()


# ============================================================
# Таблицы
# ============================================================

async def init_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS kb_qa (
            id          SERIAL PRIMARY KEY,
            question    TEXT NOT NULL,
            answer      TEXT NOT NULL,
            lang        VARCHAR(5) DEFAULT '',
            tags        TEXT DEFAULT '',
            helpful     INTEGER DEFAULT 0,
            unhelpful   INTEGER DEFAULT 0,
            active      BOOLEAN DEFAULT TRUE,
            created_by  VARCHAR(200) DEFAULT '',
            created_at  TIMESTAMP DEFAULT NOW(),
            updated_at  TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS assistant_log (
            id          SERIAL PRIMARY KEY,
            user_email  VARCHAR(200) DEFAULT '',
            channel     VARCHAR(20) DEFAULT 'web',
            question    TEXT,
            intent      VARCHAR(50) DEFAULT '',
            answer      TEXT,
            kb_id       INTEGER,
            feedback    SMALLINT,
            created_at  TIMESTAMP DEFAULT NOW()
        )
    """)
    try:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_kb_qa_q_trgm ON kb_qa USING gin (lower(question) gin_trgm_ops)")
    except Exception as e:
        print(f"[assistant] pg_trgm недоступен, поиск по базе знаний будет проще: {e}")
    # Стартовые знания: добавляем только те вопросы, которых ещё нет (правки сотрудников не затираются)
    added = 0
    for q, a, lg in SEED_KB:
        ex = await conn.fetchval("SELECT 1 FROM kb_qa WHERE lower(question)=lower($1)", q)
        if not ex:
            await conn.execute(
                "INSERT INTO kb_qa (question, answer, lang, created_by) VALUES ($1,$2,$3,'system')", q, a, lg)
            added += 1
    if added:
        print(f"[assistant] база знаний: +{added} стартовых ответов")


SEED_KB = [
    ("Jak dodać zamówienie ręcznie?",
     "Zamówienia → „+ Dodaj”. Dodaj pozycje „+ Z katalogu” (kategoria → grubość, kolor, wymiar) albo „+ Pozycja ręczna”, "
     "wpisz cenę za sztukę, dostawę i dane klienta → „Zapisz zamówienie”.", "pl"),
    ("Как добавить заказ вручную?",
     "Заказы → «+ Добавить». Позиции — «+ Из каталога» (категория → толщина, цвет, размер) или «+ Позиция вручную», "
     "цена за штуку, доставка, данные клиента → «Сохранить заказ».", "ru"),
    ("Co oznacza gwiazdka?", "Gwiazdka = priorytet (pilne). Zamówienia z gwiazdką są zawsze na górze listy.", "pl"),
    ("Что значит звёздочка?", "Звёздочка = приоритет (срочно). Такие заказы всегда наверху списка.", "ru"),
    ("Co oznacza ptaszek ✓?", "✓ = zamówienie wyeksportowane do Excela (przekazane dalej). Można odznaczyć ręcznie.", "pl"),
    ("Что значит галочка ✓?", "✓ = заказ выгружен в Excel (передан дальше). Можно снять вручную.", "ru"),
    ("Jak wyeksportować zamówienia do Excela?",
     "Zaznacz zamówienia (kwadraciki po lewej) → „Eksport zaznaczonych”. Po eksporcie dostaną ✓.", "pl"),
    ("Как выгрузить заказы в Excel?",
     "Отметь заказы (квадратики слева) → «Экспорт отмеченных». После выгрузки им ставится ✓.", "ru"),
    ("Skąd bierze się waga zamówienia?",
     "Waga = waga za sztukę × ilość, sumowana po pozycjach. Waga za sztukę jest w katalogu; jeśli jej brak — wpisuje się ręcznie.", "pl"),
    ("Откуда берётся вес заказа?",
     "Вес = вес за штуку × количество, по всем позициям. Вес за штуку — из каталога; если его нет, вписывается вручную.", "ru"),
    ("Jak cofnąć zmianę?", "Zamówienia → „Dziennik zmian”: tam jest każda zmiana i przycisk „Cofnij”.", "pl"),
    ("Как отменить изменение?", "Заказы → «Журнал изменений»: там каждое изменение и кнопка «Отменить».", "ru"),
]



SEED_KB += [
    # ── Доступ и пользователи ───────────────────────────────────
    ("Jak dodać nowego użytkownika?", "Użytkownicy → „+ Dodaj użytkownika”: imię, email, hasło, zaznacz uprawnienia i sklepy, które ma widzieć → Zapisz. Potrzebne uprawnienie „Zarządzanie użytkownikami”.", "pl"),
    ("Как добавить пользователя (сотрудника)?", "Пользователи → «+ Добавить пользователя»: имя, email, пароль, отметить права и магазины, которые он видит → Сохранить. Нужно право «Управление пользователями».", "ru"),
    ("Jak zmienić hasło?", "Hasło zmienia administrator: Użytkownicy → Edytuj przy osobie → wpisz nowe hasło → Zapisz. Puste pole = hasło bez zmian.", "pl"),
    ("Как поменять пароль?", "Пароль меняет администратор: Пользователи → «Изменить» у человека → новый пароль → Сохранить. Пустое поле — пароль не меняется.", "ru"),
    ("Dlaczego nie widzę cen albo telefonów?", "To zależy od uprawnień: „Ceny i przychody” i „Telefony klientów”. Poproś administratora, żeby je zaznaczył w Użytkownicy.", "pl"),
    ("Почему я не вижу цены или телефоны?", "Это права «Цены и выручка» и «Телефоны клиентов». Попроси администратора отметить их в разделе Пользователи.", "ru"),
    ("Dlaczego nie widzę zamówień z jakiegoś sklepu?", "Każdy użytkownik ma listę sklepów, które widzi („Dostęp do źródeł”). Administrator zmienia to w Użytkownicy → Edytuj.", "pl"),
    ("Почему я не вижу заказы какого-то магазина?", "У каждого пользователя свой список магазинов («Доступ к источникам»). Меняет администратор: Пользователи → Изменить.", "ru"),
    # ── Заказы ──────────────────────────────────────────────────
    ("Skąd biorą się zamówienia?", "Panel sam pobiera zamówienia co 15 minut: dobraszklarnia.pl, oteko.pl, cieplarnia.pl, Allegro (wszystkie konta) i Erli. Zamówienia z telefonu/maila dodaje się ręcznie („+ Dodaj”).", "pl"),
    ("Откуда берутся заказы?", "Панель сама забирает заказы каждые 15 минут: dobraszklarnia.pl, oteko.pl, cieplarnia.pl, Allegro (все аккаунты) и Erli. Заказы по телефону/почте добавляются вручную («+ Добавить»).", "ru"),
    ("Nowego zamówienia nie ma na liście", "Zamówienia przychodzą co 15 minut. Jeśli trzeba od razu — kliknij „Synchronizuj”. Sprawdź też filtry (sklep, status, daty) i wyszukiwanie.", "pl"),
    ("Нового заказа нет в списке", "Заказы приходят раз в 15 минут. Если нужно сразу — нажми «Синхронизировать». Проверь фильтры (магазин, статус, даты) и строку поиска.", "ru"),
    ("Jak edytować zamówienie?", "Edytować można tylko zamówienia ręczne: rozwiń zamówienie → „Edytuj”. Swoje — każdy, cudze — tylko admin. Zamówień ze sklepów nie edytujemy (nadpisze je synchronizacja), można dodać komentarz.", "pl"),
    ("Как отредактировать заказ?", "Редактировать можно только ручные заказы: раскрой заказ → «Изменить». Свои — любой, чужие — только админ. Заказы с магазинов не правятся (их перезапишет синхронизация), можно дописать комментарий.", "ru"),
    ("Jak usunąć zamówienie?", "Usunąć można tylko zamówienie ręczne i tylko jako superadmin: rozwiń → „Usuń”. Zamówień ze sklepów się nie usuwa.", "pl"),
    ("Как удалить заказ?", "Удалить можно только ручной заказ и только суперадмину: раскрыть → «Удалить». Заказы с магазинов не удаляются.", "ru"),
    ("Jak dodać komentarz do zamówienia?", "Kliknij w pole komentarza w wierszu zamówienia i wpisz tekst — zobaczą go wszyscy. Albo napisz do mnie: „dodaj komentarz do 18560: treść”.", "pl"),
    ("Как добавить комментарий к заказу?", "Кликни по полю комментария в строке заказа и впиши текст — его увидят все. Или напиши мне: «добавь комментарий к 18560: текст».", "ru"),
    ("Jak szukać zamówienia?", "Pole wyszukiwania (skrót: / albo Ctrl+K): nazwisko, telefon, numer zamówienia, miasto, produkt, a nawet kwota. Kilka słów naraz zawęża wynik.", "pl"),
    ("Как искать заказ?", "Строка поиска (клавиша / или Ctrl+K): фамилия, телефон, номер заказа, город, товар, даже сумма. Несколько слов сразу сужают результат.", "ru"),
    ("Czym różni się widok kompaktowy od pełnej tabeli?", "Kompaktowo — najważniejsze w jednej linii (klient, produkt, kwota, status). Pełna tabela — wszystkie kolumny: konfiguracja, waga, faktura, telefon, adres, konto.", "pl"),
    ("Чем компактный вид отличается от полной таблицы?", "Компактно — главное в одну строку (клиент, товар, сумма, статус). Полная таблица — все колонки: комплектация, вес, фактура, телефон, адрес, аккаунт.", "ru"),
    ("Co to jest konfiguracja?", "Konfiguracja to skład zamówienia dla produkcji/magazynu: wymiary, ilości, elementy zestawu. Wpisuje się przy ręcznym zamówieniu.", "pl"),
    ("Что такое комплектация (konfiguracja)?", "Комплектация — состав заказа для производства/склада: размеры, количества, элементы набора. Вписывается в ручном заказе.", "ru"),
    ("Co znaczą oznaczenia faktury?", "Faktura na firmę — klient podał NIP. Faktura imienna — fakturę na osobę prywatną. Paragon — bez faktury. Pusto — sklep tego nie przekazuje.", "pl"),
    ("Что значат пометки фактуры?", "Фактура на фирму — клиент указал NIP. Именная фактура — на частное лицо. Чек (paragon) — без фактуры. Пусто — магазин это не передаёт.", "ru"),
    ("Dlaczego zamówienie jest zielone?", "Zielone tło = zamówienie wyeksportowane (✓), czyli już przekazane dalej.", "pl"),
    ("Почему заказ зелёный?", "Зелёный фон = заказ выгружен (✓), то есть уже передан дальше.", "ru"),
    ("Jak działa pełna synchronizacja?", "„Pełna synchronizacja” pobiera ponownie wszystkie zamówienia (statusy, NIP, dane). Trwa dłużej — używaj, gdy coś się nie zgadza. Zwykła synchronizacja dzieje się sama co 15 minut.", "pl"),
    ("Что делает полная синхронизация?", "«Полная синхронизация» заново скачивает все заказы (статусы, NIP, данные). Дольше — использовать, когда что-то не сходится. Обычная идёт сама каждые 15 минут.", "ru"),
    # ── Вес, каталог ────────────────────────────────────────────
    ("Dlaczego przy wadze jest +?", "„+?” znaczy, że części pozycji brakuje wagi (np. szklarnie, profile, pozycje ręczne). Pokazana waga to suma tylko znanych pozycji.", "pl"),
    ("Почему у веса стоит +?", "«+?» значит, что у части позиций нет веса (теплицы, профили, ручные позиции). Показан вес только известных позиций.", "ru"),
    ("Do czego służy katalog?", "Katalog to lista naszych produktów: jedna nazwa, waga i kody EAN (OTEKO, EKOMAT, Allegro). Przy ręcznym zamówieniu wybierasz z niego produkt przyciskami, a zamówienia ze sklepów z kodem dostają nazwę i wagę z katalogu.", "pl"),
    ("Зачем нужен каталог?", "Каталог — список наших товаров: одно название, вес и коды EAN (OTEKO, EKOMAT, Allegro). В ручном заказе товар выбирается из него кнопками, а заказы с магазинов с кодом получают название и вес из каталога.", "ru"),
    ("Jak dodać produkt do katalogu?", "Katalog → „+ Dodaj produkt”: nazwa, kategoria, waga za sztukę, kody (jeden w linii „kod; etykieta”), parametry do przycisków → Zapisz.", "pl"),
    ("Как добавить товар в каталог?", "Каталог → «+ Добавить товар»: название, категория, вес за штуку, коды (по одному в строке «код; метка»), параметры для кнопок → Сохранить.", "ru"),
    ("Produkt nie ma wagi — co robić?", "Wpisz wagę za sztukę w zamówieniu ręcznie albo uzupełnij ją w Katalogu — wtedy będzie się wpisywać sama.", "pl"),
    ("У товара нет веса — что делать?", "Впиши вес за штуку в заказе вручную или дополни его в Каталоге — тогда он будет подставляться сам.", "ru"),
    ("Co znaczy czerwony kod w katalogu?", "Ten sam kod jest przy kilku produktach. Automatyczne dopasowanie po nim jest wyłączone — trzeba poprawić kod w katalogu.", "pl"),
    ("Что значит красный код в каталоге?", "Один и тот же код стоит у нескольких товаров. Автоподбор по нему выключен — код надо исправить в каталоге.", "ru"),
    ("Jak ukryć produkt?", "Katalog → kliknij produkt → odznacz „Aktywny”. Produkt zniknie z wyboru przy zamówieniu, ale zostanie w starych zamówieniach.", "pl"),
    ("Как скрыть товар?", "Каталог → клик по товару → снять «Активен». Он пропадёт из выбора в заказе, но останется в старых заказах.", "ru"),
    # ── Помощник и подсказки ────────────────────────────────────
    ("Jak działa katalog?", "Katalog to lista naszych produktów: jedna nazwa, waga i kody EAN. Przy ręcznym zamówieniu wybierasz produkt przyciskami (kategoria → grubość, kolor, wymiar), nazwa i waga wpisują się same.", "pl"),
    ("Как работает каталог?", "Каталог — список наших товаров: одно название, вес и коды EAN. В ручном заказе товар выбирается кнопками (категория → толщина, цвет, размер), название и вес подставляются сами.", "ru"),
    ("Dlaczego waga ma +?", "„+?” znaczy, że części pozycji brakuje wagi. Pokazana waga to suma tylko znanych pozycji.", "pl"),
    ("Что значит +? у веса?", "«+?» значит, что у части позиций нет веса. Показан вес только известных позиций.", "ru"),
    ("Jak nauczyć asystenta?", "Jeśli odpowiedź jest zła albo jej brak — kliknij 👎 albo „Naucz”, wpisz poprawną odpowiedź i zapisz. Następnym razem odpowiem tak, jak mnie nauczono.", "pl"),
    ("Как научить помощника?", "Если ответ неверный или его нет — нажми 👎 или «Научить», впиши правильный ответ и сохрани. В следующий раз отвечу так, как научили.", "ru"),
    ("Co robi przycisk ze znakiem zapytania?", "„?” w lewym górnym rogu włącza tryb pomocy: najeżdżasz na dowolny element i widzisz, co to jest i do czego służy. Kliknięcia są wtedy wyłączone. Wyjście — „?” albo Esc.", "pl"),
    ("Что делает кнопка со знаком вопроса?", "«?» слева вверху включает режим подсказок: наводишь на любой элемент и видишь, что это и зачем. Клики в этот момент отключены. Выход — «?» или Esc.", "ru"),
    ("Czy dane są bezpieczne / czy jest kopia zapasowa?", "Tak — baza kopiuje się co noc do zapasowego repozytorium, a każdą zmianę w zamówieniach widać w Dzienniku zmian i można ją cofnąć.", "pl"),
    ("Есть ли резервная копия базы?", "Да — база каждую ночь копируется в резервное хранилище, а каждое изменение заказов видно в Журнале изменений и его можно откатить.", "ru"),
]

# ============================================================
# Разбор текста
# ============================================================

PL_MAP = str.maketrans("ąćęłńóśźżĄĆĘŁŃÓŚŹŻё", "acelnoszzACELNOSZZе")


def norm(s: str) -> str:
    s = (s or "").translate(PL_MAP).lower()
    s = re.sub(r"[^\w\s#:+.,/-]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def is_ru(text: str) -> bool:
    return bool(re.search(r"[а-яё]", (text or "").lower()))


SOURCES = [
    ("allegro", ["allegro", "аллегро", "алегро"]),
    ("erli", ["erli", "эрли"]),
    ("oteko", ["oteko", "отеко", "атек"]),
    ("dobraszklarnia", ["dobrasz", "dobra szkl", "добрашкл", "доброшкл", "добра шкл", "доброй"]),
    ("cieplarnia", ["cieplarn", "цеплярн", "цепляр"]),
    ("manual", ["reczn", "ручн"]),
]

SOURCE_NAME = {"allegro": "Allegro", "erli": "Erli", "oteko": "oteko.pl", "dobraszklarnia": "dobraszklarnia.pl",
               "cieplarnia": "cieplarnia.pl", "manual": "ręczne / ручные"}


def find_sources(t: str) -> list:
    out = []
    for key, words in SOURCES:
        if any(w in t for w in words):
            out.append(key)
    return out


def find_period(t: str):
    """Возвращает (date_from, date_to, подпись PL, подпись RU) или None."""
    today = datetime.now(TZ).date()
    m = re.search(r"(\d+)\s*(dni|dnia|дн|день|дня|дней)", t)
    if m:
        n = max(1, min(366, int(m.group(1))))
        return today - timedelta(days=n - 1), today, f"ostatnie {n} dni", f"последние {n} дн."
    if any(w in t for w in ("dzis", "dzisiaj", "сегодня")):
        return today, today, "dziś", "сегодня"
    if any(w in t for w in ("wczoraj", "вчера")):
        y = today - timedelta(days=1)
        return y, y, "wczoraj", "вчера"
    if any(w in t for w in ("w tym tygodniu", "ten tydzien", "на этой неделе", "эту неделю", "этой неделе")):
        s = today - timedelta(days=today.weekday())
        return s, today, "ten tydzień", "эта неделя"
    if any(w in t for w in ("tydz", "tygodn", "недел")):
        return today - timedelta(days=6), today, "ostatnie 7 dni", "последние 7 дней"
    if any(w in t for w in ("poprzedni miesiac", "zeszly miesiac", "прошлый месяц", "прошлом месяце")):
        first = today.replace(day=1)
        last_prev = first - timedelta(days=1)
        return last_prev.replace(day=1), last_prev, "poprzedni miesiąc", "прошлый месяц"
    if any(w in t for w in ("miesiac", "месяц", "месяце")):
        return today.replace(day=1), today, "ten miesiąc", "этот месяц"
    months = [("stycz", "январ"), ("lut", "феврал"), ("marz", "март"), ("kwiet", "апрел"), ("maj", "мая"),
              ("czerw", "июн"), ("lip", "июл"), ("sierp", "август"), ("wrzes", "сентябр"), ("pazdz", "октябр"),
              ("listop", "ноябр"), ("grud", "декабр")]
    for i, (pl, rs) in enumerate(months, start=1):
        if re.search(r"\b(w |we |za |в |за )?" + pl, t) and (pl != "maj" or re.search(r"\bmaj[au]?\b", t)) or rs in t:
            y = today.year if i <= today.month else today.year - 1
            first = date(y, i, 1)
            last = (date(y + (i == 12), (i % 12) + 1, 1) - timedelta(days=1))
            names_pl = ["styczeń", "luty", "marzec", "kwiecień", "maj", "czerwiec", "lipiec", "sierpień", "wrzesień",
                        "październik", "listopad", "grudzień"]
            names_ru = ["январь", "февраль", "март", "апрель", "май", "июнь", "июль", "август", "сентябрь", "октябрь",
                        "ноябрь", "декабрь"]
            return first, min(last, today), names_pl[i - 1], names_ru[i - 1]
    if any(w in t for w in ("rok", "году", "год")):
        return today.replace(month=1, day=1), today, "ten rok", "этот год"
    return None


def find_numbers(t: str):
    """(номер заказа, телефон) из текста."""
    order_no, phone = None, None
    # размеры вида 3x6, 2100x6000, 10mm и периоды «7 dni» номерами не считаем
    clean = re.sub(r"\d+(?:[.,]\d+)?\s*x\s*\d+|\d+\s*mm\b|\d+\s*(?:dni|dnia|дн\w*|день)", " ", t)
    for raw in re.findall(r"#?\+?\d[\d -]*\d|#?\d+", clean):
        d = re.sub(r"\D", "", raw)
        if len(d) >= 9 and not raw.startswith("#") and not re.fullmatch(r"590\d{10}", d):
            phone = phone or d
        elif 3 <= len(d) <= 12:
            order_no = order_no or d
    return order_no, phone


# Слова, которые не являются именем клиента / названием товара
STOP = set("""
gdzie jest znajdz pokaz szukaj zamowienie zamowienia zamowien klient klienta klientka od dla do z ze w na i a o jak co ile
jaki jaka jakie czy mam prosze status statusie waga wazy ile ma kod ean produkt produktu towar towaru cena
где найди найти покажи заказ заказа заказы клиент клиента от для по в на и а о как что сколько какой какая какие ли
статус вес весит код товар товара продукт продукта цена мне пожалуйста у история историю истории
historia historie histori kupowal zamawial заказывал покупал
dzis dzisiaj wczoraj tydzien tygodnia miesiac сегодня вчера неделя неделю месяц
""".split())


ENDINGS = ("iego", "ego", "owi", "ach", "ami", "ow", "em", "ie", "a", "u", "y", "i", "e",
           "ого", "ему", "ой", "ом", "ым", "а", "у", "е", "ы", "и", "я", "ю")


def stem(w: str) -> str:
    """Грубая основа слова: Kowalskiego → kowalsk, Krakowa → krakow, Ковальского → ковальск."""
    for suf in ENDINGS:
        if w.endswith(suf) and len(w) - len(suf) >= 3:
            return w[:-len(suf)]
    return w


RU_LAT = {"а": "a", "б": "b", "в": "w", "г": "g", "д": "d", "е": "e", "ё": "jo", "ж": "z", "з": "z", "и": "i", "й": "j",
          "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u", "ф": "f",
          "х": "ch", "ц": "c", "ч": "cz", "ш": "sz", "щ": "szcz", "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "ju", "я": "ja"}


def lat(w: str) -> str:
    """Ковальск → kowalsk, Краков → krakow: фамилии и города в базе латиницей."""
    w = re.sub(r"(ий|ый|ой)$", "i", w)
    return "".join(RU_LAT.get(ch, ch) for ch in w)


def name_terms(t: str) -> list:
    words = [stem(w) for w in re.findall(r"[a-zа-я]{3,}", t) if w not in STOP]
    srcw = {w for _, ws in SOURCES for w in ws}
    return [w for w in words if not any(w.startswith(s) or s.startswith(w) for s in srcw)][:3]


# Намерения: (ключ, [фразы]). Порядок важен — проверяются сверху вниз.
INTENTS = [
    ("help", ["co umiesz", "pomoc", "w czym pomozesz", "что умеешь", "помощь", "что ты можешь", "help"]),
    ("action_priority", ["ustaw priorytet", "oznacz jako pilne", "oznacz pilne", "daj gwiazdke", "priorytet dla",
                         "поставь приоритет", "сделай срочн", "пометь срочн", "поставь звезд", "приоритет на"]),
    ("action_comment", ["dodaj komentarz", "dopisz komentarz", "komentarz do", "добавь комментар", "допиши комментар",
                        "комментарий к", "комментарий на"]),
    ("sync", ["synchroniz", "kiedy pobra", "ostatni sync", "синхрониз", "когда обновл", "когда подтяг", "последний синк"]),
    ("not_exported", ["niewyeksport", "nie wyeksport", "bez eksport", "niewyslan", "не выгруж", "невыгруж", "не экспорт", "без выгруз"]),
    ("priority_list", ["pilne", "priorytet", "z gwiazdk", "срочн", "приоритет", "со звезд"]),
    ("duplicates", ["duplikat", "podwojn", "dubl", "дубл", "повтор"]),
    ("order_weight", ["waga zamowienia", "ile wazy zamowienie", "вес заказа", "сколько весит заказ"]),
    ("product_weight", ["waga", "wazy", "ile waz", "вес", "весит"]),
    ("product_code", ["kod ean", "jaki kod", "ean", "код ean", "какой код", "штрихкод", "kod produktu", "код товара"]),
    ("top_products", ["najczesciej", "najlepiej sprzed", "top produkt", "co sie sprzedaje", "bestseller",
                      "популярн", "чаще всего", "лучше всего прода", "топ товар", "что продается", "что продаётся"]),
    ("delivery_city", ["dostawa do", "dostawa w", "koszt dostawy", "ile dostawa", "доставка в", "доставка до",
                       "стоимость доставки", "сколько доставка"]),
    ("avg_check", ["sredni", "srednia wartosc", "koszyk", "средний чек", "средняя сумма", "средний заказ"]),
    ("revenue", ["przychod", "obrot", "ile zarobil", "sprzedaz", "выручк", "оборот", "сколько заработ", "на сколько продали"]),
    ("by_source", ["z jakich sklepow", "wedlug sklep", "per sklep", "po sklepach", "по магазин", "с каких магазин", "по источник"]),
    ("by_status", ["po statusach", "wedlug status", "jakie statusy", "statusy", "по статус", "какие статусы", "статусы"]),
    ("customer_history", ["historia klienta", "historie klienta", "ile zamowil", "klient kupowal", "история клиента", "сколько заказывал",
                          "покупал ли", "заказывал ли"]),
    ("last_orders", ["ostatnie zamowienia", "nowe zamowienia", "najnowsze", "последние заказ", "новые заказ", "свежие заказ"]),
    ("summary", ["podsumowanie", "co dzis", "jak idzie", "raport", "сводка", "что сегодня", "как дела", "итоги", "отчет", "отчёт"]),
    ("count_orders", ["ile zamowien", "liczba zamowien", "ile bylo", "ile mamy", "сколько заказ", "количество заказ", "сколько было"]),
    ("find_order", ["gdzie", "znajdz", "pokaz zamowienie", "szukaj", "status zamowienia", "zamowienie", "где", "найди",
                    "покажи заказ", "статус заказа", "заказ"]),
]


def _has(t, *ws):
    return any(w in t for w in ws)


ORD_W = ("zamowi", "zamowie", "zamowien", "paczk", "заказ", "посылк", "посылок")
MONEY_W = ("przychod", "obrot", "kasy", "kasa ", "zarobi", "utarg", "sprzedal", "sprzedaz", "денег", "деньг", "выручк",
           "оборот", "заработ", "продали", "на какую сумму", "na jaka kwote")


def detect_intent(t: str):
    t = " " + t + " "
    cnt = bool(re.search(r"\bile\b|сколько|liczba|количеств", t))
    order = _has(t, *ORD_W)
    shop = _has(t, "sklep", "магазин", "kanal", "канал", "zrodl", "источник", "platform", "площадк")
    # 1. действия и служебное — точные фразы
    for key, phrases in INTENTS:
        if key in ("action_priority", "action_comment", "help", "sync", "duplicates", "order_weight"):
            if any(p in t for p in phrases):
                return key
    if _has(t, "gwiazdk", "звездочк", "звёздочк") and _has(t, "dodaj", "daj ", "ustaw", "postaw", "поставь", "добавь", "сделай"):
        return "action_priority"
    # 2. что делать / что отправить
    if _has(t, "do wyslania", "do wysylki", "do zrobienia", "do realizacji", "do spakowania", "отправить", "отгрузить",
            "надо сделать", "в работе", "к отправке", "собрать"):
        return "not_exported"
    if _has(t, "excel", "eksport", "выгруж", "экспорт") and _has(t, " nie ", "niewyeksport", " не ", "невыгруж", "jeszcze", "еще", "ещё"):
        return "not_exported"
    # 3. товары: вес, код, популярность
    if _has(t, "kreskow", "штрих", "barcode", "kod ean", " ean", "jaki kod", "какой код", "ma kod", "код товара", "kod produktu") \
            or (re.search(r"\b590\d{10}\b", t) and not order):
        return "product_code"
    if _has(t, "najpopularn", "популярн", "bestsell", "бестселл", "najczesciej", "чаще всего", "najlepiej sie sprzed",
            "лучше всего прода", "что продается", "что продаётся", "co sie sprzedaje", "top produkt", "топ товар"):
        return "by_source" if shop else "top_products"
    if shop and _has(t, "najwiecej", "больше всего", "najlepiej", "лучше", "ktory", "который", "какой", "po sklep", "по магазин",
                     "wedlug", "z jakich", "с каких"):
        return "by_source"
    # 4. деньги
    if _has(t, "sredni", "средн") and (_has(t, *MONEY_W) or _has(t, "koszyk", "чек", "wartosc", "сумм") or order):
        return "avg_check"
    if _has(t, "dostaw", "достав") and not cnt:
        return "delivery_city"
    if _has(t, *MONEY_W):
        return "revenue"
    # 5. остальные фразы из списка
    for key, phrases in INTENTS:
        if key in ("count_orders", "find_order"):
            continue
        if any(p in t for p in phrases):
            return key
    # 6. заказы: показать / посчитать / найти
    if cnt and (order or _has(t, "wpadl", "przyszl", "пришло", "поступил")):
        return "count_orders"
    if order and _has(t, "pokaz", "lista", "listy", "покажи", "список", "dzisiejsze", "сегодняшн", "wczorajsze", "вчерашн"):
        return "last_orders"
    if _has(t, "klient", "клиент", "kupujac", "покупател"):
        return "find_order"
    for p in dict(INTENTS)["find_order"]:
        if p in t:
            return "find_order"
    return None


# ============================================================
# Помощники вывода
# ============================================================

def money(x) -> str:
    return f"{float(x or 0):,.2f}".replace(",", " ").replace(".", ",") + " zł"


def kg(x) -> str:
    v = float(x or 0)
    s = f"{v:,.1f}" if v >= 10 else f"{v:,.2f}"
    return s.replace(",", " ").replace(".", ",") + " kg"


def pz(n) -> str:
    n = int(n or 0)
    if n == 1:
        return "1 zamówienie"
    if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14):
        return f"{n} zamówienia"
    return f"{n} zamówień"


def rz(n) -> str:
    n = int(n or 0)
    if n % 10 == 1 and n % 100 != 11:
        return f"{n} заказ"
    if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14):
        return f"{n} заказа"
    return f"{n} заказов"


def L(ru: bool, pl: str, rus: str) -> str:
    return rus if ru else pl


def source_conditions(user, params: list) -> list:
    """Ограничение по магазинам пользователя (как в /api/orders)."""
    conds = []
    us = user.get("allowed_sources", "all") or "all"
    if us != "all":
        lst = [s.strip() for s in us.split(",") if s.strip()]
        plain = [s for s in lst if ":" not in s]
        pairs = [s.split(":", 1) for s in lst if ":" in s]
        parts = []
        if plain:
            params.append(plain)
            parts.append(f"source = ANY(${len(params)})")
        for src, slr in pairs:
            params.append(src)
            params.append(slr)
            parts.append(f"(source = ${len(params)-1} AND seller_account = ${len(params)})")
        conds.append("(" + " OR ".join(parts) + ")" if parts else "FALSE")
    return conds


def base_where(user, sources=None, period=None):
    params, conds = [], ["COALESCE(status,'') NOT IN ('trash')"]
    conds += source_conditions(user, params)
    if sources:
        params.append(sources)
        conds.append(f"source = ANY(${len(params)})")
    if period:
        params.append(period[0])
        conds.append(f"COALESCE(external_created, created_at)::date >= ${len(params)}")
        params.append(period[1])
        conds.append(f"COALESCE(external_created, created_at)::date <= ${len(params)}")
    return " AND ".join(conds), params


def order_line(o, ru, can_prices, can_phones, status_pl) -> dict:
    items = []
    try:
        items = json.loads(o["items_json"] or "[]")
    except Exception:
        pass
    prod = ", ".join(f"{it.get('qty', 1)}× {it.get('full_name') or it.get('name') or ''}".strip()
                     for it in items[:2])
    if len(items) > 2:
        prod += f" +{len(items) - 2}"
    dt = o["external_created"] or o["created_at"]
    return {
        "id": o["id"],
        "title": f"{(o['customer_name'] or '—')} · {SOURCE_NAME.get(o['source'], o['source'])}"
                 + (f" #{o['external_id']}" if o["external_id"] and not str(o["external_id"]).startswith("manual") else ""),
        "sub": " · ".join(x for x in [
            dt.strftime("%d.%m %H:%M") if dt else "",
            status_pl(o["status"]),
            money(o["total"]) if can_prices else "",
            kg(o["weight_kg"]) if o["weight_kg"] is not None else "",
            (o["customer_phone"] or "") if can_phones else "",
        ] if x),
        "prod": prod,
        "search": (o["external_id"] if o["external_id"] and not str(o["external_id"]).startswith("manual")
                   else (o["customer_name"] or "")),
    }


# ============================================================
# Ответы по намерениям
# ============================================================

HELP_PL = ("Pytaj zwykłymi słowami, np.:\n"
           "• ile zamówień z Allegro w tym tygodniu?\n• przychód oteko za miesiąc\n• gdzie zamówienie Kowalski?\n"
           "• zamówienie 18560\n• ile waży szklarnia mocna 3x6 4mm?\n• kod EAN poliwęglan komorowy 10mm grafit 2100x6000\n"
           "• co się najlepiej sprzedaje w tym miesiącu?\n• niewyeksportowane zamówienia\n• pilne zamówienia\n"
           "• dostawa do Krakowa\n• historia klienta 600123456\n• podsumowanie dzisiaj\n• kiedy była synchronizacja?\n"
           "• ustaw priorytet zamówienia 18560 · dodaj komentarz do 18560: zadzwonić rano\n"
           "Jeśli czegoś nie wiem — możesz mnie nauczyć.")
HELP_RU = ("Спрашивай обычными словами, например:\n"
           "• сколько заказов с Allegro на этой неделе?\n• выручка oteko за месяц\n• где заказ Kowalski?\n"
           "• заказ 18560\n• сколько весит szklarnia mocna 3x6 4mm?\n• код EAN poliwęglan komorowy 10mm grafit 2100x6000\n"
           "• что лучше всего продаётся в этом месяце?\n• невыгруженные заказы\n• срочные заказы\n"
           "• доставка в Краков\n• история клиента 600123456\n• сводка за сегодня\n• когда была синхронизация?\n"
           "• поставь приоритет заказу 18560 · добавь комментарий к 18560: позвонить утром\n"
           "Если чего-то не знаю — можно меня научить.")


RU2PL = [("бесцветн", "bezbarwny"), ("прозрачн", "bezbarwny"), ("графит", "grafit"), ("сер", "grafit"),
         ("молочн", "mleczny"), ("опал", "mleczny"), ("бронз", "dymiony"), ("дымчат", "dymiony"),
         ("сотов", "komorowy"), ("камерн", "komorowy"), ("монолит", "lity"), ("литой", "lity"), ("литого", "lity"),
         ("теплиц", "szklarnia"), ("профил", "profil"), ("лента", "tasma"), ("ленту", "tasma"),
         ("мини", "mini"), ("компакт", "kompakt"), ("мощн", "mocna"), ("мocn", "mocna"), ("люкс", "lux"),
         ("домик", "domek"), ("большая", "duza"), ("нано", "nano"), ("бронза", "braz")]
PRODUCT_NOISE = {"waga", "wazy", "waz", "ile", "kod", "ean", "jaki", "jaka", "jakie", "plyta", "plyty", "plyt", "arkusz",
                 "kreskowy", "kreskow", "produkt", "maja", "ma", "szt", "sztuk", "sztuki", "lista", "listy", "весит",
                 "bezbarwne", "kodu", "dla", "the"}


async def find_products(conn, t: str, limit=5):
    """Товары каталога по словам вопроса (все значимые слова должны найтись в названии)."""
    t = re.sub(r"(\d)\s*мм\b", r"\1mm", t)
    t = re.sub(r"(\d)\s+mm\b", r"\1mm", t)
    t = re.sub(r"(\d)\s*[хx×]\s*(\d)", r"\1x\2", t)
    for ru_w, pl_w in RU2PL:
        t = t.replace(ru_w, pl_w)
    words = [w for w in re.findall(r"[a-z0-9,x]+", t) if len(w) >= 2 and w not in STOP and w not in PRODUCT_NOISE]
    words = [w[:6] if (len(w) > 6 and not re.search(r"\d", w)) else w for w in words]
    words = [w for w in words if not re.fullmatch(r"\d{8,14}", w)]
    if not words:
        return []
    rows = await conn.fetch("SELECT id, name, weight_kg, category FROM products WHERE active ORDER BY sort, id")
    def nm(s):
        return norm(s).replace(" mm", "mm")
    cands = []
    for r in rows:
        n = nm(r["name"])
        hit = sum(1 for w in words if w in n or (len(w) >= 5 and w.isalpha() and w[:5] in n))
        if hit:
            cands.append((hit, -len(n), r))
    if not cands:
        return []
    best = max(c[0] for c in cands)
    if best < max(1, min(2, len(words))):
        return []
    cands = [c for c in cands if c[0] == best]
    cands.sort(key=lambda c: (-c[0], -c[1]))
    return [c[2] for c in cands[:limit]], len(cands)


async def answer(user, question: str, ui_lang: str = "pl") -> dict:
    ru = is_ru(question) or (ui_lang == "ru" and not re.search(r"[a-z]{4,}", norm(question)))
    t = norm(question)
    status_pl = _ctx.get("status_pl", lambda s: s)
    can_prices = has_perm(user, "prices")
    can_phones = has_perm(user, "phones")
    if not has_perm(user, "orders"):
        return {"text": L(ru, "Brak dostępu do zamówień.", "Нет доступа к заказам."), "intent": "denied"}

    # «Как сделать… / что значит…» — это вопросы к базе знаний, а не к данным
    how = re.match(r"^(jak |jakie sa |co oznacza|co znaczy|co to |dlaczego|po co|gdzie (jest )?(przycisk|znajde|ustawic)|"
                   r"как |что значит|что такое|что это|зачем|почему|где (кнопка|найти|настро))", t)
    # Приветствия и спасибо
    if re.fullmatch(r"(czesc|hej|witam|dzien dobry|siema|hello|hi|привет|здравствуй\w*|добрый день|доброе утро|добрый вечер)[!. ]*", t):
        return {"text": L(ru, "Cześć! Zapytaj o zamówienia, produkty, wagę albo dostawę. Napisz „co umiesz”, żeby zobaczyć przykłady.",
                          "Привет! Спрашивай про заказы, товары, вес или доставку. Напиши «что умеешь» — покажу примеры."), "intent": "greet"}
    if re.fullmatch(r"(dzieki|dziekuje|dzięki|thx|thanks|спасибо|спс|благодарю)[!. ]*", t):
        return {"text": L(ru, "Proszę! 🙂", "Пожалуйста! 🙂"), "intent": "greet"}
    st = explain_status(t, ru)
    if st:
        return {"text": st, "intent": "status_explain"}
    intent = None if how else detect_intent(t)
    if how:
        pool = await _pool()
        async with pool.acquire() as conn:
            kb = await kb_search(conn, question)
        if kb:
            return {"text": kb["answer"], "kb_id": kb["id"], "kb_q": kb["question"], "intent": "kb"}
        if not re.search(r"\b(help|pomoc|umiesz|умеешь)\b", t):
            return {"text": L(ru, "Jeszcze tego nie wiem. Możesz mnie nauczyć — kliknij „Naucz” i wpisz poprawną odpowiedź.",
                              "Пока этого не знаю. Можно меня научить — нажми «Научить» и впиши правильный ответ."),
                    "teach": True, "intent": "unknown"}
        intent = "help"
    sources = find_sources(t)
    period = find_period(t)
    order_no, phone = find_numbers(t)

    if intent == "last_orders" and (order_no or phone):
        intent = "find_order"
    # Голый EAN — поиск товара по коду; голый номер/телефон — поиск заказа
    if not intent and re.fullmatch(r"\d{8,14}", re.sub(r"\s", "", t)) and len(re.sub(r"\s", "", t)) in (8, 12, 13, 14):
        intent = "product_code"
    if not intent and (order_no or phone):
        intent = "find_order"
    # Вопрос про вес с номером заказа — вес заказа
    if intent == "product_weight" and (order_no and len(order_no) >= 3) and not re.search(r"\d+\s*mm", t):
        intent = "order_weight"

    pool = await _pool()
    async with pool.acquire() as conn:
        if intent == "help":
            return {"text": HELP_RU if ru else HELP_PL, "intent": intent}

        if intent in ("count_orders", "revenue", "avg_check", "summary", "by_source", "by_status"):
            if not period:
                period = find_period("dzis") if intent == "summary" else find_period("tydzien")
            where, params = base_where(user, sources, period)
            plabel = period[3] if ru else period[2]
            src_label = ", ".join(SOURCE_NAME.get(s, s) for s in sources) if sources else L(ru, "wszystkie sklepy", "все магазины")
            if intent == "by_source":
                rows = await conn.fetch(f"SELECT source, COUNT(*) n, COALESCE(SUM(total),0) s FROM orders WHERE {where} "
                                        f"GROUP BY source ORDER BY n DESC", *params)
                lines = [f"• {SOURCE_NAME.get(r['source'], r['source'])}: {r['n']}" + (f" — {money(r['s'])}" if can_prices else "")
                         for r in rows] or [L(ru, "Brak zamówień.", "Заказов нет.")]
                return {"text": L(ru, f"Zamówienia wg sklepów ({plabel}):", f"Заказы по магазинам ({plabel}):") + "\n" + "\n".join(lines), "intent": intent}
            if intent == "by_status":
                rows = await conn.fetch(f"SELECT status, COUNT(*) n FROM orders WHERE {where} GROUP BY status ORDER BY n DESC", *params)
                lines = [f"• {status_pl(r['status'])}: {r['n']}" for r in rows] or [L(ru, "Brak zamówień.", "Заказов нет.")]
                return {"text": L(ru, f"Statusy ({plabel}, {src_label}):", f"Статусы ({plabel}, {src_label}):") + "\n" + "\n".join(lines), "intent": intent}
            r = await conn.fetchrow(f"SELECT COUNT(*) n, COALESCE(SUM(total),0) s, COALESCE(SUM(weight_kg),0) w FROM orders WHERE {where}", *params)
            n, s = r["n"], float(r["s"])
            if intent == "count_orders":
                txt = L(ru, f"{plabel.capitalize()}, {src_label}: {pz(n)}", f"{plabel.capitalize()}, {src_label}: {rz(n)}")
                if can_prices:
                    txt += L(ru, f" na {money(s)}.", f" на {money(s)}.")
                return {"text": txt, "intent": intent}
            if intent in ("revenue", "avg_check") and not can_prices:
                return {"text": L(ru, "Nie masz dostępu do cen.", "У тебя нет доступа к ценам."), "intent": intent}
            if intent == "revenue":
                return {"text": L(ru, f"Przychód — {plabel}, {src_label}: {money(s)} ({pz(n)}).",
                                  f"Выручка — {plabel}, {src_label}: {money(s)} ({rz(n)})."), "intent": intent}
            if intent == "avg_check":
                avg = s / n if n else 0
                return {"text": L(ru, f"Średnia wartość zamówienia — {plabel}, {src_label}: {money(avg)} ({pz(n)}).",
                                  f"Средний чек — {plabel}, {src_label}: {money(avg)} ({rz(n)})."), "intent": intent}
            # summary
            w2, p2 = base_where(user, sources, None)
            ne = await conn.fetchval(f"SELECT COUNT(*) FROM orders WHERE {w2} AND (exported IS NOT TRUE) "
                                     f"AND COALESCE(external_created, created_at) >= NOW() - INTERVAL '30 days'", *p2)
            pr = await conn.fetchval(f"SELECT COUNT(*) FROM orders WHERE {w2} AND is_priority = TRUE "
                                     f"AND (exported IS NOT TRUE)", *p2)
            by = await conn.fetch(f"SELECT source, COUNT(*) n FROM orders WHERE {where} GROUP BY source ORDER BY n DESC", *params)
            lines = [L(ru, f"Podsumowanie — {plabel}:", f"Сводка — {plabel}:"),
                     L(ru, f"• zamówienia: {n}", f"• заказов: {n}") + (f" — {money(s)}" if can_prices else "")]
            if float(r["w"] or 0) > 0:
                lines.append(L(ru, f"• waga (gdzie znana): {kg(r['w'])}", f"• вес (где известен): {kg(r['w'])}"))
            if by:
                lines.append("• " + ", ".join(f"{SOURCE_NAME.get(b['source'], b['source'])} {b['n']}" for b in by))
            lines.append(L(ru, f"• niewyeksportowane (30 dni): {ne}", f"• не выгружено (30 дней): {ne}"))
            lines.append(L(ru, f"• pilne do zrobienia: {pr}", f"• срочных в работе: {pr}"))
            return {"text": "\n".join(lines), "intent": intent}

        if intent == "not_exported" or intent == "priority_list" or intent == "last_orders":
            where, params = base_where(user, sources, period)
            if intent == "not_exported":
                extra = " AND (exported IS NOT TRUE) AND COALESCE(external_created, created_at) >= NOW() - INTERVAL '30 days'"
                head = L(ru, "Niewyeksportowane (ostatnie 30 dni)", "Не выгружены (последние 30 дней)")
            elif intent == "priority_list":
                extra = " AND is_priority = TRUE AND (exported IS NOT TRUE)"
                head = L(ru, "Pilne (z gwiazdką, jeszcze nie wyeksportowane)", "Срочные (со звёздочкой, ещё не выгружены)")
            else:
                extra = ""
                head = L(ru, "Ostatnie zamówienia", "Последние заказы")
            cnt = await conn.fetchval(f"SELECT COUNT(*) FROM orders WHERE {where}{extra}", *params)
            rows = await conn.fetch(f"SELECT * FROM orders WHERE {where}{extra} "
                                    f"ORDER BY COALESCE(external_created, created_at) DESC LIMIT 8", *params)
            return {"text": f"{head}: {cnt}" + (L(ru, " (pokazuję 8 najnowszych)", " (показываю 8 последних)") if cnt > 8 else ""),
                    "orders": [order_line(o, ru, can_prices, can_phones, status_pl) for o in rows], "intent": intent}

        if intent == "duplicates":
            where, params = base_where(user, sources, period or find_period("30 dni"))
            rows = await conn.fetch(f"""
                SELECT right(regexp_replace(customer_phone,'[^0-9]','','g'),9) ph, COUNT(*) n,
                       string_agg(COALESCE(customer_name,'?') || ' (' || source || ')', ', ') who
                FROM orders WHERE {where} AND length(regexp_replace(COALESCE(customer_phone,''),'[^0-9]','','g')) >= 9
                GROUP BY 1 HAVING COUNT(*) > 1 ORDER BY n DESC LIMIT 10""", *params)
            if not rows:
                return {"text": L(ru, "Nie widzę powtórzonych zamówień (ten sam telefon) w tym okresie.",
                                  "Повторов (один телефон) за период не вижу."), "intent": intent}
            lines = [f"• {('…' + r['ph'][-4:]) if not can_phones else r['ph']}: {r['n']} — {r['who'][:120]}" for r in rows]
            return {"text": L(ru, "Kilka zamówień z tego samego telefonu (możliwe duplikaty albo stali klienci):",
                              "Несколько заказов с одного телефона (возможные дубли или постоянные клиенты):") + "\n" + "\n".join(lines),
                    "intent": intent}

        if intent == "sync":
            rows = await conn.fetch("SELECT DISTINCT ON (source) source, status, orders_new, finished_at FROM sync_log "
                                    "ORDER BY source, finished_at DESC NULLS LAST")
            if not rows:
                return {"text": L(ru, "Brak informacji o synchronizacji.", "Нет данных о синхронизации."), "intent": intent}
            lines = [f"• {r['source']}: {r['finished_at'].strftime('%d.%m %H:%M') if r['finished_at'] else '—'} — {r['status']}"
                     + (f", +{r['orders_new']}" if r['orders_new'] else "") for r in rows]
            return {"text": L(ru, "Ostatnia synchronizacja:", "Последняя синхронизация:") + "\n" + "\n".join(lines), "intent": intent}

        if intent in ("product_weight", "product_code"):
            code = re.search(r"\b(\d{8,14})\b", t)
            if code:
                rows = await conn.fetch("SELECT DISTINCT p.name, p.weight_kg FROM product_codes pc JOIN products p ON p.id=pc.product_id "
                                        "WHERE pc.code=$1", code.group(1))
                if not rows:
                    return {"text": L(ru, f"Kodu {code.group(1)} nie ma w katalogu.", f"Кода {code.group(1)} нет в каталоге."), "intent": intent}
                lines = [f"• {r['name']}" + (f" — {kg(r['weight_kg'])}" if r["weight_kg"] is not None else "") for r in rows]
                warn = L(ru, "\n⚠ Ten kod jest przy kilku produktach.", "\n⚠ Этот код стоит у нескольких товаров.") if len(rows) > 1 else ""
                return {"text": L(ru, f"Kod {code.group(1)}:", f"Код {code.group(1)}:") + "\n" + "\n".join(lines) + warn, "intent": intent}
            res = await find_products(conn, t)
            if not res:
                return {"text": L(ru, "Nie znalazłem takiego produktu w katalogu. Napisz np.: „waga poliwęglan lity 6mm bezbarwny 2050x2000”.",
                                  "Не нашёл такой товар в каталоге. Напиши, например: «вес poliwęglan lity 6mm bezbarwny 2050x2000»."),
                        "intent": intent}
            prods, total = res
            lines = []
            mq = re.search(r"\b(\d{1,4})\s*(szt|sztuk|plyt|arkusz|шт|лист|плит)", t)
            qty = int(mq.group(1)) if mq else 1
            for p in prods:
                if intent == "product_weight":
                    if p["weight_kg"] is None:
                        lines.append(f"• {p['name']} — " + L(ru, "brak wagi w katalogu", "веса в каталоге нет"))
                    elif qty > 1:
                        lines.append(f"• {p['name']} — {kg(p['weight_kg'])} × {qty} = {kg(float(p['weight_kg']) * qty)}")
                    else:
                        lines.append(f"• {p['name']} — {kg(p['weight_kg'])}")
                else:
                    cs = await conn.fetch("SELECT code, label FROM product_codes WHERE product_id=$1 ORDER BY id", p["id"])
                    lines.append(f"• {p['name']}: " + (", ".join(f"{c['code']} {c['label']}".strip() for c in cs) or "—"))
            more = L(ru, f"\n…i jeszcze {total - len(prods)}. Doprecyzuj grubość, kolor albo wymiar.",
                     f"\n…и ещё {total - len(prods)}. Уточни толщину, цвет или размер.") if total > len(prods) else ""
            return {"text": "\n".join(lines) + more, "intent": intent}

        if intent == "top_products":
            where, params = base_where(user, sources, period or find_period("miesiac"))
            plabel = (period or find_period("miesiac"))[3 if ru else 2]
            rows = await conn.fetch(f"""
                SELECT COALESCE(it->>'full_name', it->>'name') AS name,
                       SUM(COALESCE(NULLIF(it->>'qty','')::numeric, 1)) AS q, COUNT(DISTINCT o.id) AS n
                FROM orders o, jsonb_array_elements(CASE WHEN o.items_json ~ '^\\s*\\[' THEN o.items_json::jsonb ELSE '[]'::jsonb END) it
                WHERE {where} AND COALESCE(it->>'full_name', it->>'name','') <> ''
                  {"AND lower(COALESCE(it->>'full_name', it->>'name')) LIKE '%szklarn%'" if _has(t, "szklarn", "теплиц") else ""}
                  {"AND lower(COALESCE(it->>'full_name', it->>'name')) LIKE '%poliw%'" if _has(t, "poliw", "поликарб", "plyt", "лист") else ""}
                GROUP BY 1 ORDER BY n DESC, q DESC LIMIT 10""", *params)
            if not rows:
                return {"text": L(ru, "Brak danych o sprzedanych produktach w tym okresie.", "Нет данных о проданных товарах за период."), "intent": intent}
            lines = [f"{i+1}. {r['name'][:90]} — {r['n']} " + L(ru, "zam.", "зак.") + f", {float(r['q']):g} " + L(ru, "szt.", "шт.")
                     for i, r in enumerate(rows)]
            return {"text": L(ru, f"Najczęściej kupowane ({plabel}):", f"Чаще всего покупают ({plabel}):") + "\n" + "\n".join(lines), "intent": intent}

        if intent == "delivery_city":
            words = [w for w in re.findall(r"[a-zа-я]{3,}", t) if w not in STOP and w not in
                     ("dostawa", "koszt", "dostawy", "доставка", "стоимость", "доставки", "сколько", "ile")]
            if not words:
                return {"text": L(ru, "Podaj miasto, np. „dostawa do Krakowa”.", "Назови город, например «доставка в Краков»."), "intent": intent}
            city = words[-1]
            stem = lat(city)[:max(4, len(lat(city)) - 2)]
            where, params = base_where(user, sources, None)
            params.append("%" + stem + "%")
            r = await conn.fetchrow(f"""SELECT COUNT(*) n, AVG(shipping_cost) a, MIN(shipping_cost) mn, MAX(shipping_cost) mx
                FROM orders WHERE {where} AND shipping_cost > 0 AND
                (translate(lower(COALESCE(customer_city,'')),'ąćęłńóśźż','acelnoszz') ILIKE ${len(params)})""", *params)
            if not r or not r["n"] or not can_prices:
                return {"text": L(ru, f"Nie mam danych o dostawie do „{city}”.", f"Нет данных о доставке в «{city}»."), "intent": intent}
            return {"text": L(ru, f"Dostawa do „{city}”: średnio {money(r['a'])}, od {money(r['mn'])} do {money(r['mx'])} ({r['n']} zam.).",
                              f"Доставка в «{city}»: в среднем {money(r['a'])}, от {money(r['mn'])} до {money(r['mx'])} ({r['n']} зак.)."),
                    "intent": intent}

        if intent in ("find_order", "customer_history", "order_weight"):
            where, params = base_where(user, sources, period)
            conds = []
            if phone:
                params.append("%" + phone[-9:])
                conds.append(f"regexp_replace(COALESCE(customer_phone,''),'[^0-9]','','g') LIKE ${len(params)}")
            elif order_no:
                params.append(order_no)
                conds.append(f"(external_id = ${len(params)} OR id::text = ${len(params)} OR external_id LIKE '%' || ${len(params)})")
            else:
                terms = name_terms(t)
                if not terms:
                    return {"text": L(ru, "Podaj nazwisko, telefon albo numer zamówienia.", "Назови фамилию, телефон или номер заказа."), "intent": intent}
                for w in terms:
                    params.append("%" + w + "%")
                    a = len(params)
                    params.append("%" + lat(w) + "%")
                    b = len(params)
                    hay = ("translate(lower(COALESCE(customer_name,'') || ' ' || COALESCE(customer_email,'') || ' ' || "
                           "COALESCE(customer_city,'')),'ąćęłńóśźż','acelnoszz')")
                    conds.append(f"({hay} ILIKE ${a} OR {hay} ILIKE ${b})")
            q = f"SELECT * FROM orders WHERE {where} AND " + " AND ".join(conds)
            rows = await conn.fetch(q + " ORDER BY COALESCE(external_created, created_at) DESC LIMIT 8", *params)
            if not rows:
                return {"text": L(ru, "Nie znalazłem takiego zamówienia.", "Такой заказ не нашёл."), "intent": intent}
            if intent == "order_weight":
                o = rows[0]
                return {"text": L(ru, "Waga zamówienia: ", "Вес заказа: ") + (kg(o["weight_kg"]) if o["weight_kg"] is not None
                                                                           else L(ru, "nieznana (brak wag pozycji)", "неизвестен (у позиций нет веса)")),
                        "orders": [order_line(o, ru, can_prices, can_phones, status_pl)], "intent": intent}
            if intent == "customer_history":
                agg = await conn.fetchrow(q.replace("SELECT *", "SELECT COUNT(*) n, COALESCE(SUM(total),0) s, "
                                                    "MIN(COALESCE(external_created, created_at)) f"), *params)
                txt = L(ru, f"Klient: {pz(agg['n'])} od {agg['f'].strftime('%d.%m.%Y')}",
                        f"Клиент: {rz(agg['n'])} с {agg['f'].strftime('%d.%m.%Y')}")
                if can_prices:
                    txt += L(ru, f", razem {money(agg['s'])}", f", всего {money(agg['s'])}")
                return {"text": txt + ".", "orders": [order_line(o, ru, can_prices, can_phones, status_pl) for o in rows], "intent": intent}
            return {"text": L(ru, f"Znalazłem: {len(rows)}" + (" (pierwsze 8)" if len(rows) == 8 else ""),
                              f"Нашёл: {len(rows)}" + (" (первые 8)" if len(rows) == 8 else "")),
                    "orders": [order_line(o, ru, can_prices, can_phones, status_pl) for o in rows], "intent": intent}

        if intent in ("action_priority", "action_comment"):
            if not order_no:
                return {"text": L(ru, "Podaj numer zamówienia, np. „ustaw priorytet 18560” albo „dodaj komentarz do 18560: …”.",
                                  "Укажи номер заказа, например «поставь приоритет 18560» или «добавь комментарий к 18560: …»."), "intent": intent}
            where, params = base_where(user)
            params.append(order_no)
            o = await conn.fetchrow(f"SELECT * FROM orders WHERE {where} AND (external_id = ${len(params)} OR id::text = ${len(params)}) "
                                    f"ORDER BY id DESC LIMIT 1", *params)
            if not o:
                return {"text": L(ru, f"Nie znalazłem zamówienia {order_no}.", f"Заказ {order_no} не нашёл."), "intent": intent}
            if intent == "action_priority":
                act = {"type": "priority", "order_id": o["id"], "value": True}
                desc = L(ru, "Ustawić priorytet (★) dla zamówienia", "Поставить приоритет (★) заказу")
            else:
                if not has_perm(user, "edit_comments"):
                    return {"text": L(ru, "Nie masz prawa edytować komentarzy.", "Нет права править комментарии."), "intent": intent}
                m = re.search(r":\s*(.+)$", question.strip())
                text = (m.group(1).strip() if m else "")
                if not text:
                    return {"text": L(ru, "Napisz treść po dwukropku: „dodaj komentarz do 18560: zadzwonić rano”.",
                                      "Напиши текст после двоеточия: «добавь комментарий к 18560: позвонить утром»."), "intent": intent}
                act = {"type": "comment", "order_id": o["id"], "value": text}
                desc = L(ru, f"Dopisać komentarz „{text}” do zamówienia", f"Дописать комментарий «{text}» к заказу")
            token = secrets.token_urlsafe(12)
            _pending[token] = (time.time(), user.get("email", ""), act)
            return {"text": f"{desc}:", "orders": [order_line(o, ru, can_prices, can_phones, status_pl)],
                    "confirm": {"token": token, "label": L(ru, "Tak, zrób", "Да, сделать")}, "intent": intent}

        # ── База знаний ─────────────────────────────────────────
        kb = await kb_search(conn, question)
        if kb:
            return {"text": kb["answer"], "kb_id": kb["id"], "kb_q": kb["question"], "intent": "kb"}

    return {"text": L(ru, "Jeszcze tego nie wiem. Możesz mnie nauczyć — kliknij „Naucz” i wpisz poprawną odpowiedź. "
                          "Napisz „co umiesz”, żeby zobaczyć przykłady pytań.",
                      "Пока этого не знаю. Можно меня научить — нажми «Научить» и впиши правильный ответ. "
                      "Напиши «что умеешь», чтобы увидеть примеры вопросов."),
            "teach": True, "intent": "unknown"}


KB_GENERIC = {"jak", "jaki", "jaka", "jakie", "dodac", "zrobic", "zmienic", "gdzie", "znalezc", "mozna", "oznacza", "znaczy",
              "dlaczego", "czemu", "kiedy", "добавить", "сделать", "поменять", "изменить", "найти", "можно", "значит",
              "означает", "почему", "зачем", "когда", "нужен", "нужно", "нужна", "работает", "dziala", "jest", "есть",
              "który", "ktory", "который", "какой", "какая"}


KB_STOP = set("""dla jest jaki jaka jakie przez przy oraz albo tylko jeszcze moze mozna prosze
это этот эта как что для при или только еще ещё можно пожалуйста чтобы""".split())


async def kb_search(conn, question: str):
    q = (question or "").strip().lower()
    keyw = {w[:5] for w in re.findall(r"\w{4,}", norm(q)) if w not in KB_STOP and w not in KB_GENERIC}
    try:
        rows = await conn.fetch("""
            SELECT id, question, answer, similarity(lower(question), $1) AS sim FROM kb_qa
            WHERE active AND similarity(lower(question), $1) > 0.25
            ORDER BY sim DESC, helpful - unhelpful DESC LIMIT 8""", q)
        for r in rows:
            kw = {w[:5] for w in re.findall(r"\w{4,}", norm(r["question"])) if w not in KB_STOP and w not in KB_GENERIC}
            common = keyw & kw
            # Похоже по буквам И совпадает хотя бы одно смысловое слово (иначе «добавить пользователя» ≈ «добавить заказ»)
            if r["sim"] >= 0.75 or (common and (r["sim"] >= 0.3 or len(common) >= 2)) and len(common) >= max(1, len(keyw) // 2):
                return dict(r)
    except Exception:
        pass
    # Второй проход: совпадение смысловых слов с вопросом И ответом (ловит перефразировки:
    # «kto ma dyżur w soboty» ↔ «kto odbiera telefon w sobotę» / «W soboty dyżur ma Marek»)
    if not keyw:
        return None
    rows = await conn.fetch("SELECT id, question, answer, helpful, unhelpful FROM kb_qa WHERE active")
    best, score = None, 0.0
    for r in rows:
        kq = {w[:5] for w in re.findall(r"\w{4,}", norm(r["question"])) if w not in KB_STOP and w not in KB_GENERIC}
        ka = {w[:5] for w in re.findall(r"\w{4,}", norm(r["answer"])) if w not in KB_STOP and w not in KB_GENERIC}
        cq, ca = keyw & kq, (keyw & ka) - kq
        if not cq:
            continue
        sc = len(cq) + 0.6 * len(ca) + 0.05 * (r["helpful"] - r["unhelpful"])
        if sc > score:
            best, score = r, sc
    need = max(1.6, 0.6 * len(keyw))
    if best and score >= need:
        return dict(best)
    return None


_pending = {}  # token -> (ts, email, action)


# Статусы: подписи как в панели + что это значит
STATUS_INFO = {
    "awaiting_cod": ("Płatność przy odbiorze", "Оплата при получении",
                     "Klient zapłaci kurierowi przy odbiorze (pobranie). Zamówienie można wysyłać.",
                     "Клиент платит курьеру при получении (наложенный платёж). Заказ можно отправлять."),
    "awaiting_proforma": ("Oczekujemy na opłacenie Proformy", "Ожидание оплаты проформы",
                          "Wysłaliśmy proformę — czekamy na przelew. Nie wysyłać przed wpłatą.",
                          "Отправили проформу — ждём перевод. Не отправлять до оплаты."),
    "processing": ("W realizacji", "В обработке", "Zamówienie przyjęte i opłacone (albo za pobraniem) — do przygotowania i wysyłki.",
                   "Заказ принят и оплачен (или наложенный платёж) — собрать и отправить."),
    "payment_accepted": ("Płatność zaakceptowana", "Оплата принята", "Pieniądze wpłynęły — można realizować.", "Деньги пришли — можно выполнять."),
    "payment_received": ("Płatność przyjęta", "Оплата получена", "Pieniądze wpłynęły — można realizować.", "Деньги пришли — можно выполнять."),
    "przelewy24_accepted": ("Płatność Przelewy24 przyjęta", "Оплата Przelewy24 принята", "Opłacone online przez Przelewy24 — można realizować.",
                            "Оплачено онлайн через Przelewy24 — можно выполнять."),
    "awaiting_bankwire": ("Oczekiwanie na przelew", "Ожидание перевода", "Klient wybrał przelew i jeszcze nie zapłacił. Nie wysyłać przed wpłatą.",
                          "Клиент выбрал перевод и ещё не оплатил. Не отправлять до оплаты."),
    "pending_payment": ("Oczekiwanie na płatność", "Ожидание оплаты", "Płatność nie została jeszcze dokończona.", "Оплата ещё не завершена."),
    "awaiting_payment": ("Oczekiwanie na płatność", "Ожидание оплаты", "Płatność nie została jeszcze dokończona.", "Оплата ещё не завершена."),
    "on-hold": ("Wstrzymane", "На удержании", "Wstrzymane — np. czeka na potwierdzenie przelewu albo wyjaśnienie z klientem.",
                "Приостановлен — например, ждёт подтверждения перевода или уточнения с клиентом."),
    "shipped": ("Wysłane", "Отправлен", "Paczka wysłana do klienta.", "Посылка отправлена клиенту."),
    "delivered": ("Dostarczone", "Доставлен", "Klient odebrał zamówienie.", "Клиент получил заказ."),
    "completed": ("Zrealizowane", "Выполнен", "Zamówienie zakończone.", "Заказ завершён."),
    "cancelled": ("Anulowane", "Отменён", "Zamówienie anulowane — nie realizować.", "Заказ отменён — не выполнять."),
    "failed": ("Nieudane", "Ошибка", "Płatność online się nie udała. Warto zadzwonić do klienta — często chce kupić, tylko płatność nie przeszła.",
               "Онлайн-оплата не прошла. Стоит позвонить клиенту — часто он хочет купить, просто платёж сорвался."),
    "payment_error": ("Błąd płatności", "Ошибка оплаты", "Płatność się nie udała — zadzwonić do klienta.", "Оплата не прошла — позвонить клиенту."),
    "refunded": ("Zwrócone", "Возврат", "Pieniądze zwrócone klientowi.", "Деньги возвращены клиенту."),
    "bought": ("Kupione", "Куплено", "Allegro: klient kupił, ale jeszcze nie wypełnił/nie opłacił formularza dostawy.",
               "Allegro: клиент купил, но ещё не заполнил/не оплатил форму доставки."),
    "filled_in": ("Wypełnione", "Заполнено", "Allegro: klient wypełnił dane dostawy, czeka na płatność.", "Allegro: клиент заполнил доставку, ждём оплату."),
    "ready_for_processing": ("Gotowe do realizacji", "Готово к выполнению", "Allegro: opłacone — można wysyłać.", "Allegro: оплачено — можно отправлять."),
}


STATUS_ALIASES = {
    "processing": ["w realizacji", "в реализации", "в обработке", "в работе"],
    "awaiting_cod": ["oczekiwanie na platnosc przy odbiorze", "platnosc przy odbiorze", "za pobraniem", "pobranie",
                     "оплата при получении", "наложенн"],
    "awaiting_proforma": ["proform", "проформ"],
    "shipped": ["wyslane", "отправлен"],
    "cancelled": ["anulowan", "отменен", "отменён"],
    "on-hold": ["wstrzyman", "на удержании", "приостановлен"],
    "failed": ["nieudane", "ошибка оплаты", "не прошла оплата"],
}


def explain_status(t: str, ru: bool):
    if not _has(" " + t + " ", "status", "статус", "co znaczy", "co oznacza", "что значит", "что означает", "co to jest",
                "что такое", "co to "):
        return None
    best = None
    for key, (pl, rs, epl, ers) in STATUS_INFO.items():
        for label in (pl, rs, key.replace("_", " "), *STATUS_ALIASES.get(key, [])):
            n = norm(label)
            if n and n in t and (not best or len(n) > best[0]):
                best = (len(n), pl, rs, epl, ers)
    if not best:
        return None
    _, pl, rs, epl, ers = best
    return f"«{pl}» ({rs}) — {ers}" if ru else f"„{pl}” — {epl}"


# ============================================================
# API
# ============================================================

async def _log(conn, user, channel, question, res):
    return await conn.fetchval(
        "INSERT INTO assistant_log (user_email, channel, question, intent, answer, kb_id) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id",
        user.get("email", ""), channel, question[:2000], res.get("intent", ""), (res.get("text") or "")[:4000], res.get("kb_id"))


async def ask_and_log(user, question: str, lang: str = "pl", channel: str = "web") -> dict:
    question = (question or "").strip()[:1000]
    if not question:
        return {"text": ""}
    try:
        res = await answer(user, question, lang)
    except Exception as e:
        print(f"[assistant] error on {question!r}: {e}")
        res = {"text": "Coś poszło nie tak. / Что-то пошло не так.", "intent": "error"}
    pool = await _pool()
    async with pool.acquire() as conn:
        res["log_id"] = await _log(conn, user, channel, question, res)
    return res


@router.post("/api/assistant/ask")
async def api_ask(request: Request):
    user = await get_current_user(request)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    body = await request.json()
    return JSONResponse(await ask_and_log(user, body.get("q", ""), body.get("lang", "pl")))


@router.post("/api/assistant/confirm")
async def api_confirm(request: Request):
    user = await get_current_user(request)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    body = await request.json()
    item = _pending.pop(body.get("token", ""), None)
    if not item or time.time() - item[0] > 600 or item[1] != user.get("email", ""):
        return JSONResponse({"ok": False, "error": "expired"})
    act = item[2]
    pool = await _pool()
    log_action, label = _ctx["log_action"], _ctx["order_label"]
    async with pool.acquire() as conn:
        oid = int(act["order_id"])
        if act["type"] == "priority":
            if not has_perm(user, "orders"):
                return JSONResponse({"ok": False, "error": "denied"})
            was = await conn.fetchval("SELECT is_priority FROM orders WHERE id=$1", oid)
            await conn.execute("UPDATE orders SET is_priority=TRUE WHERE id=$1", oid)
            if not was:
                await log_action(conn, user, "priority", [oid], old_value="0", new_value="1", label=await label(conn, oid))
        elif act["type"] == "comment":
            if not has_perm(user, "edit_comments"):
                return JSONResponse({"ok": False, "error": "denied"})
            was = await conn.fetchval("SELECT customer_comment FROM orders WHERE id=$1", oid) or ""
            new = (was + ("\n" if was else "") + act["value"]).strip()
            await conn.execute("UPDATE orders SET customer_comment=$1 WHERE id=$2", new, oid)
            await log_action(conn, user, "comment", [oid], old_value=was, new_value=new, label=await label(conn, oid))
    return JSONResponse({"ok": True})


@router.post("/api/assistant/feedback")
async def api_feedback(request: Request):
    user = await get_current_user(request)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    body = await request.json()
    good = 1 if body.get("good") else -1
    pool = await _pool()
    async with pool.acquire() as conn:
        r = await conn.fetchrow("UPDATE assistant_log SET feedback=$1 WHERE id=$2 AND user_email=$3 RETURNING kb_id",
                                good, int(body.get("log_id") or 0), user.get("email", ""))
        if r and r["kb_id"]:
            col = "helpful" if good > 0 else "unhelpful"
            await conn.execute(f"UPDATE kb_qa SET {col}={col}+1 WHERE id=$1", r["kb_id"])
    return JSONResponse({"ok": True})


@router.post("/api/assistant/teach")
async def api_teach(request: Request):
    """Любой сотрудник может научить: вопрос + правильный ответ → база знаний."""
    user = await get_current_user(request)
    if not user or not has_perm(user, "orders"):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    body = await request.json()
    q, a = (body.get("question") or "").strip(), (body.get("answer") or "").strip()
    if len(q) < 3 or len(a) < 2:
        return JSONResponse({"ok": False, "error": "empty"})
    pool = await _pool()
    async with pool.acquire() as conn:
        kid = await conn.fetchval("INSERT INTO kb_qa (question, answer, lang, created_by) VALUES ($1,$2,$3,$4) RETURNING id",
                                  q[:500], a[:4000], "ru" if is_ru(q) else "pl", user.get("email", ""))
    return JSONResponse({"ok": True, "id": kid})


def _kb_admin(user):
    return user and (has_perm(user, "manage_users") or has_perm(user, "assistant_kb"))


@router.get("/assistant", response_class=HTMLResponse)
async def kb_page(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)
    if not _kb_admin(user):
        return RedirectResponse("/orders", status_code=302)
    return templates.TemplateResponse(request=request, name="assistant_kb.html", context={"user": user})


@router.get("/api/assistant/kb")
async def api_kb_list(request: Request):
    user = await get_current_user(request)
    if not _kb_admin(user):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    pool = await _pool()
    async with pool.acquire() as conn:
        kb = await conn.fetch("SELECT * FROM kb_qa ORDER BY active DESC, id DESC")
        # Вопросы, на которые последний раз не было ответа или ответ получил 👎, и которых нет в базе знаний
        unk = await conn.fetch("""
            WITH last AS (SELECT DISTINCT ON (lower(question)) lower(question) lq, intent, feedback
                          FROM assistant_log ORDER BY lower(question), id DESC)
            SELECT min(a.question) question, COUNT(*) n, MAX(a.created_at) last
            FROM assistant_log a JOIN last l ON l.lq = lower(a.question)
            WHERE (l.intent = 'unknown' OR l.feedback < 0)
              AND NOT EXISTS (SELECT 1 FROM kb_qa k WHERE k.active AND lower(k.question) = l.lq)
            GROUP BY l.lq ORDER BY n DESC, last DESC LIMIT 50""")
        stats = await conn.fetchrow("""SELECT COUNT(*) total,
            COUNT(*) FILTER (WHERE intent NOT IN ('unknown','error')) answered,
            COUNT(*) FILTER (WHERE feedback > 0) good, COUNT(*) FILTER (WHERE feedback < 0) bad
            FROM assistant_log WHERE created_at > NOW() - INTERVAL '30 days'""")
    def d(r):
        x = dict(r)
        for k, v in x.items():
            if isinstance(v, (datetime, date)):
                x[k] = v.isoformat()
        return x
    return JSONResponse({"kb": [d(r) for r in kb], "unknown": [d(r) for r in unk], "stats": d(stats)})


@router.post("/api/assistant/kb")
async def api_kb_save(request: Request):
    user = await get_current_user(request)
    if not _kb_admin(user):
        return JSONResponse({"error": "Access denied"}, status_code=403)
    b = await request.json()
    q, a = (b.get("question") or "").strip(), (b.get("answer") or "").strip()
    if not q or not a:
        return JSONResponse({"ok": False, "error": "empty"})
    pool = await _pool()
    async with pool.acquire() as conn:
        if b.get("id"):
            await conn.execute("UPDATE kb_qa SET question=$1, answer=$2, active=$3, updated_at=NOW() WHERE id=$4",
                               q, a, bool(b.get("active", True)), int(b["id"]))
        else:
            await conn.execute("INSERT INTO kb_qa (question, answer, lang, created_by) VALUES ($1,$2,$3,$4)",
                               q, a, "ru" if is_ru(q) else "pl", user.get("email", ""))
    return JSONResponse({"ok": True})
