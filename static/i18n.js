/* EKOMAT Dashboard — язык (PL/RU) и тема (светлая/тёмная).
   Подключать в конце <body>:  <script src="/static/i18n.js"></script>

   Разметка:
     <span data-i18n="ключ">…</span>          — заменит текст
     <input data-i18n-ph="ключ">              — заменит placeholder
     <button data-i18n-title="ключ">          — заменит title
     <div data-i18n-html="ключ">              — заменит innerHTML
   Переключатели:
     <button data-lang="pl">PL</button>
     <button data-lang="ru">RU</button>
     <button data-theme-toggle>☾</button>
*/
(function (w, d) {
  'use strict';

  var LS_LANG = 'ekomat_lang';
  var LS_THEME = 'ekomat_theme';

  var DICT = {
    pl: {
      panel: 'Panel analityczny',
      theme: 'Motyw',
      signin: 'Zaloguj się do konta',
      email: 'Email',
      password: 'Hasło',
      signin_btn: 'Zaloguj się',
      rights: 'Wszystkie prawa zastrzeżone',
      na_title: 'Brak dostępu',
      na_text: 'Twoje konto nie ma przypisanych uprawnień do tej strony.<br>Skontaktuj się z administratorem, aby uzyskać dostęp.',
      logout: 'Wyloguj się',
      old_ui: 'Stara wersja'
    },
    ru: {
      panel: 'Аналитическая панель',
      theme: 'Тема',
      signin: 'Вход в аккаунт',
      email: 'Email',
      password: 'Пароль',
      signin_btn: 'Войти',
      rights: 'Все права защищены',
      na_title: 'Нет доступа',
      na_text: 'У вашего аккаунта нет прав на эту страницу.<br>Обратитесь к администратору, чтобы получить доступ.',
      logout: 'Выйти',
      old_ui: 'Старая версия'
    }
  };

  function safeGet(key, def) {
    try { return w.localStorage.getItem(key) || def; } catch (e) { return def; }
  }
  function safeSet(key, val) {
    try { w.localStorage.setItem(key, val); } catch (e) {}
  }

  var lang = safeGet(LS_LANG, 'pl');
  if (lang !== 'pl' && lang !== 'ru') lang = 'pl';
  var theme = safeGet(LS_THEME, 'light');
  if (theme !== 'light' && theme !== 'dark') theme = 'light';

  function t(key) {
    var pack = DICT[lang] || DICT.pl;
    if (pack[key] != null) return pack[key];
    if (DICT.pl[key] != null) return DICT.pl[key];
    return key;
  }

  function applyLang() {
    d.documentElement.setAttribute('lang', lang);
    each('[data-i18n]', function (el) { el.textContent = t(el.getAttribute('data-i18n')); });
    each('[data-i18n-html]', function (el) { el.innerHTML = t(el.getAttribute('data-i18n-html')); });
    each('[data-i18n-ph]', function (el) { el.setAttribute('placeholder', t(el.getAttribute('data-i18n-ph'))); });
    each('[data-i18n-title]', function (el) { el.setAttribute('title', t(el.getAttribute('data-i18n-title'))); });
    each('[data-lang]', function (el) {
      el.classList.toggle('is-active', el.getAttribute('data-lang') === lang);
    });
  }

  function applyTheme() {
    d.documentElement.setAttribute('data-theme', theme);
    each('[data-theme-toggle]', function (el) { el.textContent = theme === 'dark' ? '\u2600' : '\u263E'; });
  }

  function each(sel, fn) {
    var list = d.querySelectorAll(sel);
    for (var i = 0; i < list.length; i++) fn(list[i]);
  }

  function setLang(next) {
    if (next !== 'pl' && next !== 'ru') return;
    lang = next;
    safeSet(LS_LANG, next);
    applyLang();
    if (typeof w.onEkomatLangChange === 'function') w.onEkomatLangChange(next);
  }

  function toggleTheme() {
    theme = theme === 'dark' ? 'light' : 'dark';
    safeSet(LS_THEME, theme);
    applyTheme();
    if (typeof w.onEkomatThemeChange === 'function') w.onEkomatThemeChange(theme);
  }

  d.addEventListener('click', function (ev) {
    var el = ev.target;
    while (el && el !== d.body) {
      if (el.hasAttribute && el.hasAttribute('data-lang')) { setLang(el.getAttribute('data-lang')); return; }
      if (el.hasAttribute && el.hasAttribute('data-theme-toggle')) { toggleTheme(); return; }
      el = el.parentNode;
    }
  });

  w.EKOMAT = {
    t: t,
    get lang() { return lang; },
    get theme() { return theme; },
    setLang: setLang,
    toggleTheme: toggleTheme,
    dict: DICT,
    /* добавить переводы со страницы: EKOMAT.extend({pl:{...},ru:{...}}) */
    extend: function (extra) {
      ['pl', 'ru'].forEach(function (k) {
        if (!extra[k]) return;
        for (var n in extra[k]) if (Object.prototype.hasOwnProperty.call(extra[k], n)) DICT[k][n] = extra[k][n];
      });
      applyLang();
    },
    refresh: applyLang
  };

  applyTheme();
  if (d.readyState === 'loading') d.addEventListener('DOMContentLoaded', applyLang);
  else applyLang();
})(window, document);
