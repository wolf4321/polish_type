/* EKOMAT — режим подсказок «?».
   Кнопка «?» в левом верхнем углу. Включил — наводишь на любую кнопку, поле, колонку или окно
   и видишь, что это и зачем. Пока режим включён, клики ничего не делают: можно спокойно изучать.
   Выход — ещё раз «?» или Esc.

   Подключение: <script src="/static/help.js" defer></script> перед </body> на любой странице.
   Тексты — в RULES ниже: [CSS-селектор, текст PL, текст RU]. Берётся самый «глубокий» совпавший
   элемент, поэтому общие правила (например, .modal) можно писать рядом с точными (#m-shipping). */
(function () {
  'use strict';
  if (window.__ekHelp) return;
  window.__ekHelp = 1;

  function lang() { try { return localStorage.getItem('ekomat_lang') === 'ru' ? 'ru' : 'pl'; } catch (e) { return 'pl'; } }

  var RULES = [
    // ── Общее: меню ─────────────────────────────────────────────
    ['.nav-logo', 'EKOMAT — panel zamówień ze wszystkich sklepów i Allegro w jednym miejscu.', 'EKOMAT — панель заказов со всех магазинов и Allegro в одном месте.'],
    ['.nav-links a[href="/"]', 'Pulpit: liczby sprzedaży, wykres dzienny, źródła, kampanie. Do szybkiej oceny, jak idzie.', 'Сводка: продажи, график по дням, источники, кампании. Чтобы быстро понять, как идут дела.'],
    ['.nav-links a[href="/orders"]', 'Zamówienia: wszystkie zamówienia ze sklepów, Allegro, Erli i ręczne. Tu pracujesz na co dzień.', 'Заказы: все заказы с магазинов, Allegro, Erli и ручные. Основное рабочее место.'],
    ['.nav-links a[href="/products"]', 'Katalog produktów: nazwy, kody EAN i waga. Z niego wybierasz towar przy ręcznym zamówieniu.', 'Каталог товаров: названия, коды EAN и вес. Из него выбирается товар в ручном заказе.'],
    ['.nav-links a[href="/admin/users"]', 'Użytkownicy: kto ma dostęp do panelu i co może robić.', 'Пользователи: у кого есть доступ к панели и что ему можно.'],
    ['[data-lang]', 'Język panelu: polski albo rosyjski. Dane się nie zmieniają, tylko napisy.', 'Язык панели: польский или русский. Меняются только надписи, данные те же.'],
    ['.theme-toggle', 'Jasny / ciemny motyw.', 'Светлая / тёмная тема.'],
    ['.nav-user', 'Twoje konto.', 'Твой аккаунт.'],
    ['.logout', 'Wyloguj się z panelu.', 'Выйти из панели.'],

    // ── Заказы: карточки и фильтры ─────────────────────────────
    ['#stats-row', 'Liczby za wybrany okres (daty w filtrze): ile zamówień i z którego sklepu.', 'Цифры за выбранный период (даты в фильтре): сколько заказов и с какого магазина.'],
    ['#s-total', 'Wszystkie zamówienia w wybranym okresie i filtrach.', 'Все заказы за выбранный период и с учётом фильтров.'],
    ['#s-revenue', 'Suma zamówień (z dostawą) w wybranym okresie.', 'Сумма заказов (с доставкой) за период.'],
    ['#s-woo', 'Zamówienia ze sklepu dobraszklarnia.pl.', 'Заказы с dobraszklarnia.pl.'],
    ['#s-oteko', 'Zamówienia ze sklepu oteko.pl.', 'Заказы с oteko.pl.'],
    ['#s-ciep', 'Zamówienia ze sklepu cieplarnia.pl.', 'Заказы с cieplarnia.pl.'],
    ['#s-allegro', 'Zamówienia z Allegro (wszystkie konta).', 'Заказы с Allegro (все аккаунты).'],
    ['#s-erli', 'Zamówienia z Erli.', 'Заказы с Erli.'],
    ['[data-view="compact"]', 'Widok kompaktowy: najważniejsze w jednej linii — klient, produkt, kwota, status.', 'Компактный вид: главное в одну строку — клиент, товар, сумма, статус.'],
    ['[data-view="full"]', 'Pełna tabela: wszystkie kolumny, w tym konfiguracja, waga, telefon, adres. Szerokość kolumn można zmieniać, przeciągając krawędź nagłówka.', 'Полная таблица: все колонки, включая комплектацию, вес, телефон, адрес. Ширину колонок можно менять, перетаскивая край заголовка.'],
    ['#f-search', 'Szukaj po nazwisku, telefonie, numerze zamówienia, mieście albo produkcie. Skrót: klawisz /.', 'Поиск по фамилии, телефону, номеру заказа, городу или товару. Быстрая клавиша: /.'],
    ['#search-clear', 'Wyczyść wyszukiwanie.', 'Очистить поиск.'],
    ['#f-source-wrap', 'Z których sklepów pokazać zamówienia. Można wybrać kilka.', 'С каких магазинов показывать заказы. Можно выбрать несколько.'],
    ['#f-status', 'Pokaż tylko zamówienia w wybranym statusie.', 'Показать только заказы в выбранном статусе.'],
    ['#f-from', 'Data od — początek okresu.', 'Дата с — начало периода.'],
    ['#f-to', 'Data do — koniec okresu.', 'Дата по — конец периода.'],
    ['button[onclick="loadOrders(1)"]', 'Zastosuj filtry i odśwież listę.', 'Применить фильтры и обновить список.'],
    ['#more-btn', 'Więcej filtrów: wyeksportowane, priorytet, typ towaru, faktura.', 'Ещё фильтры: выгруженные, приоритет, тип товара, фактура.'],
    ['#f-exported', 'Pokaż tylko już wyeksportowane (✓) albo jeszcze nie.', 'Показать только уже выгруженные (✓) или ещё нет.'],
    ['#f-priority', 'Pokaż tylko zamówienia z gwiazdką (pilne).', 'Показать только заказы со звёздочкой (срочные).'],
    ['#f-product-type', 'Filtr po typie towaru: szklarnia, poliwęglan, akcesoria.', 'Фильтр по типу товара: теплица, поликарбонат, аксессуары.'],
    ['#f-invoice', 'Filtr po dokumencie: faktura na firmę, imienna, paragon.', 'Фильтр по документу: фактура на фирму, именная, чек.'],
    ['#btn-export-sel', 'Pobierz zaznaczone zamówienia do Excela. Po eksporcie dostaną znaczek ✓.', 'Скачать отмеченные заказы в Excel. После выгрузки им ставится ✓.'],
    ['#btn-clear-sel', 'Odznacz wszystkie zaznaczone zamówienia.', 'Снять отметку со всех заказов.'],
    ['#btn-sync', 'Pobierz nowe zamówienia ze sklepów teraz (normalnie dzieje się to samo co 15 minut).', 'Подтянуть новые заказы с магазинов сейчас (обычно это происходит само раз в 15 минут).'],
    ['#btn-full-sync', 'Pełne odświeżenie: pobiera ponownie wszystkie zamówienia (NIP, statusy). Trwa dłużej — używaj rzadko.', 'Полное обновление: заново скачивает все заказы (NIP, статусы). Долго — пользоваться редко.'],
    ['button[onclick="openAddModal()"]', 'Dodaj zamówienie ręcznie — np. z telefonu albo maila.', 'Добавить заказ вручную — например, по телефону или из почты.'],
    ['#sync-info', 'Kiedy ostatnio pobrano zamówienia z każdego sklepu i czy bez błędów.', 'Когда последний раз забирались заказы с каждого магазина и были ли ошибки.'],
    ['button[onclick="toggleExportHistory()"]', 'Dziennik zmian: kto, co i kiedy zmienił. Większość zmian można cofnąć.', 'Журнал изменений: кто, что и когда менял. Большинство изменений можно откатить.'],

    // ── Заказы: таблица ─────────────────────────────────────────
    ['.select-all', 'Zaznacz wszystkie zamówienia na tej stronie.', 'Отметить все заказы на странице.'],
    ['.order-check', 'Zaznacz zamówienie — np. do eksportu do Excela.', 'Отметить заказ — например, для выгрузки в Excel.'],
    ['.star-btn', 'Priorytet: pilne zamówienie. Zamówienia z gwiazdką są zawsze na górze listy.', 'Приоритет: срочный заказ. Заказы со звёздочкой всегда наверху списка.'],
    ['.exported-check', '✓ = zamówienie już wyeksportowane do Excela (przekazane dalej). Można odznaczyć ręcznie.', '✓ = заказ уже выгружен в Excel (передан дальше). Можно снять вручную.'],
    ['.c-date', 'Data i godzina zamówienia.', 'Дата и время заказа.'],
    ['.c-prod', 'Co zamówiono: ilość × produkt.', 'Что заказали: количество × товар.'],
    ['.editable-comment', 'Komentarz do zamówienia. Kliknij (poza trybem pomocy), żeby dopisać — zobaczą go wszyscy.', 'Комментарий к заказу. Кликни (вне режима подсказок), чтобы дописать — его увидят все.'],
    ['.c-amount', 'Kwota zamówienia, dostawa i waga zamówienia (kg). „+?” przy wadze — część pozycji nie ma wagi.', 'Сумма заказа, доставка и вес заказа (кг). «+?» у веса — у части позиций вес неизвестен.'],
    ['.w-cell', 'Waga zamówienia w kg (waga/szt. × ilość). Dla logistyki — ile ładunku w aucie. „+?” — części pozycji brakuje wagi.', 'Вес заказа в кг (вес/шт × кол-во). Для логиста — сколько груза в машине. «+?» — у части позиций нет веса.'],
    ['.pill', 'Status zamówienia.', 'Статус заказа.'],
    ['.c-pay', 'Sposób płatności.', 'Способ оплаты.'],
    ['.c-src', 'Skąd przyszło zamówienie: sklep albo konto Allegro.', 'Откуда пришёл заказ: магазин или аккаунт Allegro.'],
    ['.c-chev', 'Rozwiń szczegóły zamówienia.', 'Раскрыть детали заказа.'],
    ['.config-cell', 'Konfiguracja: skład zamówienia / wymiary — jak dla produkcji.', 'Комплектация: состав заказа / размеры — как для производства.'],
    ['.phone-cell', 'Telefon klienta.', 'Телефон клиента.'],
    ['.address-cell', 'Adres dostawy.', 'Адрес доставки.'],
    ['.seller-cell', 'Konto sprzedawcy (dla Allegro — które konto).', 'Аккаунт продавца (для Allegro — какой аккаунт).'],
    ['.order-row', 'Zamówienie. Kliknij (poza trybem pomocy), żeby zobaczyć szczegóły: klient, dostawa, płatność, pozycje.', 'Заказ. Кликни (вне режима подсказок), чтобы увидеть детали: клиент, доставка, оплата, позиции.'],
    ['.detail-panel', 'Szczegóły zamówienia: klient, dostawa, płatność, dokumenty i lista pozycji z wagą.', 'Детали заказа: клиент, доставка, оплата, документы и список позиций с весом.'],
    ['.row-act-edit', 'Edytuj ręczne zamówienie (tylko swoje; admin — wszystkie).', 'Редактировать ручной заказ (только свои; админ — все).'],
    ['.row-act-del', 'Usuń ręczne zamówienie (tylko admin).', 'Удалить ручной заказ (только админ).'],
    ['#pagination', 'Strony listy i ile zamówień pokazywać na stronie.', 'Страницы списка и сколько заказов показывать на странице.'],

    // ── Ручной заказ ────────────────────────────────────────────
    ['#m-date', 'Data i godzina zamówienia.', 'Дата и время заказа.'],
    ['#m-items', 'Pozycje zamówienia. Cena — za sztukę, brutto. Waga — za sztukę; z katalogu wpisuje się sama, brakującą wpisz ręcznie.', 'Позиции заказа. Цена — за штуку, брутто. Вес — за штуку; из каталога подставляется сам, если нет — впиши руками.'],
    ['.mi-qty', 'Ilość sztuk.', 'Количество штук.'],
    ['.mi-price', 'Cena za sztukę brutto (z VAT). Wpisuj ręcznie — sprawdź z ceną na stronie.', 'Цена за штуку брутто (с НДС). Вводится вручную — сверь с ценой на сайте.'],
    ['.mi-w', 'Waga jednej sztuki w kg. Pomarańczowa ramka — produkt z katalogu bez wagi, wpisz ją.', 'Вес одной штуки в кг. Оранжевая рамка — товар из каталога без веса, впиши его.'],
    ['.mi-del', 'Usuń pozycję.', 'Удалить позицию.'],
    ['button[onclick="togglePicker()"]', 'Dodaj produkt z katalogu: kategoria → grubość, kolor, wymiar → kliknij produkt. Nazwa i waga wpiszą się same.', 'Добавить товар из каталога: категория → толщина, цвет, размер → клик по товару. Название и вес подставятся сами.'],
    ['button[onclick*="addItemRow"]', 'Pozycja spoza katalogu: reklamacja, niestandardowy wymiar. Nazwę i wagę wpisujesz sam.', 'Позиция не из каталога: рекламация, нестандартный размер. Название и вес вписываешь сам.'],
    ['#pk-search', 'Szukaj w katalogu po nazwie albo kodzie EAN.', 'Поиск по каталогу: название или код EAN.'],
    ['.pk-chip', 'Wybór parametru. Kolejne przyciski pokazują tylko to, co istnieje dla wybranych parametrów.', 'Выбор параметра. Следующие кнопки показывают только то, что есть для уже выбранного.'],
    ['.pk-res', 'Kliknij, żeby dodać ten produkt do zamówienia.', 'Кликни, чтобы добавить этот товар в заказ.'],
    ['#m-sum', 'Podsumowanie: suma towarów, razem z dostawą i waga całego zamówienia.', 'Итог: сумма товаров, итого с доставкой и вес всего заказа.'],
    ['#m-type', 'Typ towaru do kolumny w tabeli. Puste — panel ustawi sam po pozycjach.', 'Тип товара для колонки в таблице. Пусто — панель определит сама по позициям.'],
    ['#m-config', 'Konfiguracja: szczegóły dla produkcji / magazynu.', 'Комплектация: детали для производства / склада.'],
    ['#m-shipping', 'Koszt dostawy — jeden na całe zamówienie.', 'Стоимость доставки — одна на весь заказ.'],
    ['#m-status', 'Status zamówienia.', 'Статус заказа.'],
    ['#m-payment', 'Sposób płatności: gotówka, przelew, Blik…', 'Способ оплаты: наличные, перевод, Blik…'],
    ['#m-name', 'Imię i nazwisko klienta.', 'Имя и фамилия клиента.'],
    ['#m-phone', 'Telefon klienta.', 'Телефон клиента.'],
    ['#m-email', 'Email klienta.', 'Email клиента.'],
    ['#m-city', 'Miasto dostawy.', 'Город доставки.'],
    ['#m-address', 'Adres dostawy.', 'Адрес доставки.'],
    ['#m-seller', 'Konto sprzedawcy, jeśli zamówienie dotyczy konkretnego konta (np. Allegro).', 'Аккаунт продавца, если заказ относится к конкретному аккаунту (например, Allegro).'],
    ['#m-comment', 'Komentarz — widoczny w tabeli zamówień.', 'Комментарий — виден в таблице заказов.'],
    ['.btn-save', 'Zapisz zamówienie.', 'Сохранить заказ.'],
    ['.btn-cancel', 'Zamknij bez zapisywania.', 'Закрыть без сохранения.'],

    // ── Сводка ──────────────────────────────────────────────────
    ['#d-total', 'Liczba zamówień w wybranym okresie.', 'Количество заказов за выбранный период.'],
    ['#d-revenue', 'Przychód w wybranym okresie.', 'Выручка за период.'],
    ['#d-avg', 'Średnia wartość zamówienia.', 'Средний чек.'],
    ['#d-channels', 'Ile kanałów sprzedaży działało w okresie.', 'Сколько каналов продаж работало за период.'],
    ['#dailyChart', 'Zamówienia i przychód dzień po dniu.', 'Заказы и выручка по дням.'],
    ['#source-bars', 'Udział każdego sklepu w zamówieniach.', 'Доля каждого магазина в заказах.'],
    ['#devices-container', 'Z jakich urządzeń kupują: telefon, komputer.', 'С каких устройств покупают: телефон, компьютер.'],
    ['#source-body', 'Szczegóły po źródłach: zamówienia, przychód, średni koszyk.', 'Детали по источникам: заказы, выручка, средний чек.'],
    ['#channels-container', 'Skąd przychodzą klienci w każdym sklepie: Google, reklama, bezpośrednio…', 'Откуда приходят покупатели в каждом магазине: Google, реклама, напрямую…'],
    ['#campaigns-container', 'Kampanie reklamowe, z których przyszły zamówienia.', 'Рекламные кампании, с которых пришли заказы.'],
    ['#f-source', 'Wybierz sklep, aby zobaczyć tylko jego liczby.', 'Выбери магазин, чтобы смотреть только его цифры.'],

    // ── Каталог ─────────────────────────────────────────────────
    ['#q', 'Szukaj produktu po nazwie albo kodzie.', 'Поиск товара по названию или коду.'],
    ['#cat', 'Pokaż jedną kategorię.', 'Показать одну категорию.'],
    ['#show-off', 'Pokaż też ukryte produkty (np. chwilowo niedostępne).', 'Показать и скрытые товары (например, временно нет в наличии).'],
    ['#only-dup', 'Tylko produkty z kodem, który stoi przy kilku produktach — do poprawienia.', 'Только товары с кодом, который стоит у нескольких товаров — их надо поправить.'],
    ['.pr-name', 'Kliknij, żeby edytować: nazwa, kody, waga, parametry.', 'Кликни, чтобы редактировать: название, коды, вес, параметры.'],
    ['.pr-codes', 'Kody produktu (EAN OTEKO, EKOMAT, Allegro…). Czerwony ⚠ — ten sam kod jest przy innym produkcie, automatyczne dopasowanie po nim nie działa.', 'Коды товара (EAN OTEKO, EKOMAT, Allegro…). Красный ⚠ — тот же код стоит у другого товара, автоподбор по нему не работает.'],
    ['#f-name', 'Nazwa produktu — dokładnie tak pojawi się w zamówieniach.', 'Название товара — именно так оно будет в заказах.'],
    ['#f-cat', 'Kategoria — pierwszy przycisk przy wyborze z katalogu.', 'Категория — первая кнопка при выборе из каталога.'],
    ['#f-w', 'Waga jednej sztuki w kg. Z niej liczy się waga zamówienia.', 'Вес одной штуки в кг. Из него считается вес заказа.'],
    ['#f-type', 'Typ towaru do kolumny w tabeli zamówień.', 'Тип товара для колонки в таблице заказов.'],
    ['#f-codes', 'Kody: jeden w linii, „kod; etykieta”. Zamówienie ze sklepu z takim kodem dostanie nazwę i wagę z katalogu.', 'Коды: по одному в строке, «код; метка». Заказ с сайта с таким кодом получит название и вес из каталога.'],
    ['#f-attrs', 'Parametry do przycisków wyboru: „Grubość: 10 mm”, „Kolor: Grafit”…', 'Параметры для кнопок выбора: «Grubość: 10 mm», «Kolor: Grafit»…'],
    ['#f-active', 'Aktywny — widoczny przy dodawaniu zamówienia. Odznacz, żeby ukryć bez usuwania.', 'Активен — виден при добавлении заказа. Сними, чтобы скрыть без удаления.'],
    ['button[onclick^="openEdit(null)"]', 'Dodaj nowy produkt do katalogu.', 'Добавить новый товар в каталог.'],

    // ── Пользователи ────────────────────────────────────────────
    ['#users-table', 'Lista użytkowników i ich uprawnienia.', 'Список пользователей и их права.'],
    ['.perm-check', 'Uprawnienie: zaznacz, co ta osoba może widzieć i robić.', 'Право: отметь, что этому человеку можно видеть и делать.'],
    ['.src-check', 'Z których sklepów ta osoba widzi zamówienia.', 'С каких магазинов этот человек видит заказы.'],
    ['#backup-section', 'Kopie zapasowe bazy: robią się co noc; można zrobić ręcznie albo pobrać.', 'Резервные копии базы: делаются каждую ночь; можно сделать вручную или скачать.'],

    // ── Общие блоки (в конце — менее точные) ────────────────────
    ['.modal', 'Okno formularza. Zamknij przyciskiem albo klawiszem Esc.', 'Окно формы. Закрыть кнопкой или клавишей Esc.'],
    ['table', 'Tabela. Nagłówki mówią, co jest w kolumnach.', 'Таблица. Заголовки подсказывают, что в колонках.'],
    ['.stat-card', 'Karta z liczbą za wybrany okres.', 'Карточка с цифрой за выбранный период.']
  ];

  var UI = {
    pl: { btn: 'Pomoc: najedź na element, żeby zobaczyć, co to jest', on: 'Tryb pomocy: najedź na dowolny element. Kliknięcia są wyłączone. Wyjście — „?” albo Esc.', none: 'Brak opisu dla tego elementu.' },
    ru: { btn: 'Подсказки: наведи на элемент, чтобы узнать, что это', on: 'Режим подсказок: наведи на любой элемент. Клики отключены. Выход — «?» или Esc.', none: 'Для этого элемента описания нет.' }
  };

  var css = ''
    + '.ek-help-btn{position:fixed;left:12px;top:14px;z-index:10001;width:30px;height:30px;border-radius:50%;'
    + 'border:1px solid var(--line-strong,#ccc);background:var(--card-bg,#fff);color:var(--ink,#111);font:700 15px/28px "IBM Plex Sans",sans-serif;'
    + 'cursor:pointer;text-align:center;padding:0;box-shadow:0 1px 3px rgba(0,0,0,.12);}'
    + '.ek-help-btn:hover{border-color:var(--ink,#111);}'
    + '.ek-help-btn.on{background:var(--btn-bg,#111);color:var(--btn-ink,#fff);border-color:var(--btn-bg,#111);}'
    + '.nav{padding-left:54px !important;}'
    + 'body.ek-help-on, body.ek-help-on *{cursor:help !important;}'
    + '.ek-help-hl{outline:2px solid #2563eb !important;outline-offset:2px;border-radius:4px;}'
    + '.ek-help-tip{position:fixed;z-index:10002;max-width:320px;background:#111827;color:#fff;font:13px/1.45 "IBM Plex Sans",sans-serif;'
    + 'padding:9px 12px;border-radius:9px;box-shadow:0 6px 24px rgba(0,0,0,.25);pointer-events:none;display:none;}'
    + '.ek-help-bar{position:fixed;left:50%;bottom:16px;transform:translateX(-50%);z-index:10002;background:#2563eb;color:#fff;'
    + 'font:600 12.5px/1.4 "IBM Plex Sans",sans-serif;padding:8px 14px;border-radius:20px;box-shadow:0 4px 16px rgba(0,0,0,.2);display:none;max-width:90vw;text-align:center;}';

  var on = false, btn, tip, bar, hl = null;

  function t(k) { return UI[lang()][k]; }

  function findRule(el) {
    // самый глубокий элемент, для которого есть правило
    var node = el;
    while (node && node !== document.body && node.nodeType === 1) {
      if (node.getAttribute && node.getAttribute('data-help')) return { el: node, text: node.getAttribute('data-help') };
      for (var i = 0; i < RULES.length; i++) {
        try {
          if (node.matches(RULES[i][0])) return { el: node, text: RULES[i][lang() === 'ru' ? 2 : 1] };
        } catch (e) {}
      }
      node = node.parentElement;
    }
    return null;
  }

  function place(x, y) {
    var w = tip.offsetWidth, h = tip.offsetHeight, vw = window.innerWidth, vh = window.innerHeight;
    var left = x + 16, top = y + 18;
    if (left + w > vw - 8) left = Math.max(8, x - w - 12);
    if (top + h > vh - 8) top = Math.max(8, y - h - 12);
    tip.style.left = left + 'px';
    tip.style.top = top + 'px';
  }

  function clearHl() { if (hl) { hl.classList.remove('ek-help-hl'); hl = null; } }

  function onMove(e) {
    if (!on) return;
    if (e.target === btn) { tip.style.display = 'none'; clearHl(); return; }
    var r = findRule(e.target);
    clearHl();
    if (!r) {
      tip.textContent = t('none');
      tip.style.opacity = '.75';
    } else {
      tip.textContent = r.text;
      tip.style.opacity = '1';
      hl = r.el;
      hl.classList.add('ek-help-hl');
    }
    tip.style.display = 'block';
    place(e.clientX, e.clientY);
  }

  function block(e) {
    if (!on) return;
    if (e.target === btn || (e.target.closest && e.target.closest('[data-lang]'))) return;
    e.preventDefault();
    e.stopPropagation();
    if (e.stopImmediatePropagation) e.stopImmediatePropagation();
  }

  function toggle(force) {
    on = (typeof force === 'boolean') ? force : !on;
    document.body.classList.toggle('ek-help-on', on);
    btn.classList.toggle('on', on);
    bar.textContent = t('on');
    bar.style.display = on ? 'block' : 'none';
    if (!on) { tip.style.display = 'none'; clearHl(); }
  }

  function init() {
    var st = document.createElement('style');
    st.textContent = css;
    document.head.appendChild(st);

    btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'ek-help-btn';
    btn.textContent = '?';
    btn.title = t('btn');
    btn.setAttribute('aria-label', t('btn'));
    btn.addEventListener('click', function (e) { e.stopPropagation(); toggle(); });
    document.body.appendChild(btn);

    tip = document.createElement('div');
    tip.className = 'ek-help-tip';
    document.body.appendChild(tip);

    bar = document.createElement('div');
    bar.className = 'ek-help-bar';
    document.body.appendChild(bar);

    document.addEventListener('mousemove', onMove, true);
    ['click', 'mousedown', 'mouseup', 'dblclick', 'submit', 'change', 'focusin'].forEach(function (ev) {
      document.addEventListener(ev, block, true);
    });
    // На телефоне: тап показывает подсказку вместо действия
    document.addEventListener('touchstart', function (e) {
      if (!on || e.target === btn) return;
      var p = e.touches && e.touches[0];
      if (p) onMove({ target: e.target, clientX: p.clientX, clientY: p.clientY });
    }, { capture: true, passive: true });
    document.addEventListener('keydown', function (e) { if (on && e.key === 'Escape') toggle(false); }, true);
    // смена языка — обновить тексты
    document.addEventListener('click', function (e) {
      if (e.target.closest && e.target.closest('[data-lang]')) setTimeout(function () {
        btn.title = t('btn'); bar.textContent = t('on');
      }, 50);
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
