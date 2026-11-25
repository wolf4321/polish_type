/* EKOMAT — помощник (окно чата справа внизу).
   Отвечает по живым данным панели и по базе знаний. Не знает — предлагает научить.
   Подключение: <script src="/static/assistant.js" defer></script> перед </body>. */
(function () {
  'use strict';
  if (window.__ekAsst) return;
  window.__ekAsst = 1;

  function lang() { try { return localStorage.getItem('ekomat_lang') === 'ru' ? 'ru' : 'pl'; } catch (e) { return 'pl'; } }
  var T = {
    pl: { title: 'Asystent', ph: 'Zapytaj, np. „ile zamówień z Allegro dziś?”', send: 'Wyślij', hello: 'Cześć! Pytaj o zamówienia, produkty, wagę, dostawę. Napisz „co umiesz”, żeby zobaczyć przykłady.',
          teach: 'Naucz', teachQ: 'Pytanie', teachA: 'Poprawna odpowiedź', save: 'Zapisz', saved: 'Dziękuję, zapamiętałem.', open: 'Otwórz', err: 'Błąd połączenia.', done: 'Zrobione ✓', expired: 'Potwierdzenie wygasło, zapytaj jeszcze raz.',
          chips: ['podsumowanie dzisiaj', 'niewyeksportowane zamówienia', 'pilne zamówienia', 'co się najlepiej sprzedaje w tym miesiącu', 'co umiesz'] },
    ru: { title: 'Помощник', ph: 'Спроси, например «сколько заказов с Allegro сегодня?»', send: 'Отправить', hello: 'Привет! Спрашивай про заказы, товары, вес, доставку. Напиши «что умеешь», чтобы увидеть примеры.',
          teach: 'Научить', teachQ: 'Вопрос', teachA: 'Правильный ответ', save: 'Сохранить', saved: 'Спасибо, запомнил.', open: 'Открыть', err: 'Ошибка связи.', done: 'Готово ✓', expired: 'Подтверждение устарело, спроси ещё раз.',
          chips: ['сводка за сегодня', 'невыгруженные заказы', 'срочные заказы', 'что лучше всего продаётся в этом месяце', 'что умеешь'] }
  };
  function t(k) { return T[lang()][k]; }
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }

  var css = ''
    + '.ek-as-fab{position:fixed;right:18px;bottom:18px;z-index:9000;height:46px;padding:0 18px;border-radius:23px;border:none;cursor:pointer;'
    + 'background:var(--btn-bg,#111);color:var(--btn-ink,#fff);font:600 13.5px "IBM Plex Sans",sans-serif;box-shadow:0 6px 20px rgba(0,0,0,.22);}'
    + '.ek-as{position:fixed;right:18px;bottom:76px;z-index:9001;width:390px;max-width:calc(100vw - 24px);height:560px;max-height:calc(100vh - 100px);'
    + 'background:var(--card-bg,#fff);color:var(--ink,#111);border:1px solid var(--line,#e5e7eb);border-radius:14px;box-shadow:0 14px 40px rgba(0,0,0,.25);'
    + 'display:none;flex-direction:column;overflow:hidden;font:13.5px/1.45 "IBM Plex Sans",sans-serif;}'
    + '.ek-as.open{display:flex;}'
    + '.ek-as-h{display:flex;align-items:center;justify-content:space-between;padding:11px 14px;border-bottom:1px solid var(--line,#e5e7eb);font-weight:600;}'
    + '.ek-as-h button{background:none;border:none;font-size:18px;cursor:pointer;color:var(--muted,#666);}'
    + '.ek-as-m{flex:1;overflow:auto;padding:12px;display:flex;flex-direction:column;gap:8px;}'
    + '.ek-b{max-width:88%;padding:8px 11px;border-radius:11px;white-space:pre-wrap;word-wrap:break-word;}'
    + '.ek-b.u{align-self:flex-end;background:var(--btn-bg,#111);color:var(--btn-ink,#fff);border-bottom-right-radius:4px;}'
    + '.ek-b.a{align-self:flex-start;background:var(--chip,#f3f4f6);border-bottom-left-radius:4px;}'
    + '.ek-o{display:block;margin-top:6px;padding:6px 8px;border-radius:8px;background:var(--card-bg,#fff);border:1px solid var(--line,#e5e7eb);cursor:pointer;white-space:normal;}'
    + '.ek-o:hover{border-color:var(--line-strong,#999);}'
    + '.ek-o b{font-weight:600;display:block;font-size:12.5px;}'
    + '.ek-o span{font-size:11.5px;color:var(--muted,#666);display:block;}'
    + '.ek-fb{display:flex;gap:6px;margin-top:5px;}'
    + '.ek-fb button,.ek-chip,.ek-act{background:var(--card-bg,#fff);border:1px solid var(--line,#e5e7eb);border-radius:7px;padding:3px 8px;cursor:pointer;font:12px "IBM Plex Sans",sans-serif;color:var(--ink,#111);}'
    + '.ek-fb button.on{background:var(--btn-bg,#111);color:var(--btn-ink,#fff);}'
    + '.ek-act{background:var(--btn-bg,#111);color:var(--btn-ink,#fff);border-color:var(--btn-bg,#111);margin-top:6px;padding:5px 12px;}'
    + '.ek-chips{display:flex;flex-wrap:wrap;gap:5px;padding:0 12px 8px;}'
    + '.ek-as-f{display:flex;gap:6px;padding:10px;border-top:1px solid var(--line,#e5e7eb);}'
    + '.ek-as-f input{flex:1;padding:9px 11px;border:1px solid var(--line,#e5e7eb);border-radius:9px;font:13.5px "IBM Plex Sans",sans-serif;background:var(--input-bg,#fff);color:var(--ink,#111);}'
    + '.ek-as-f button{padding:0 14px;border:none;border-radius:9px;background:var(--btn-bg,#111);color:var(--btn-ink,#fff);cursor:pointer;font-weight:600;}'
    + '.ek-teach{margin-top:6px;display:flex;flex-direction:column;gap:5px;}'
    + '.ek-teach input,.ek-teach textarea{padding:6px 8px;border:1px solid var(--line,#e5e7eb);border-radius:7px;font:12.5px "IBM Plex Sans",sans-serif;background:var(--input-bg,#fff);color:var(--ink,#111);}'
    + '@media (max-width:600px){.ek-as{right:6px;left:6px;width:auto;bottom:70px;height:70vh;}.ek-as-fab{right:10px;bottom:10px;}}';

  var box, msgs, input, chipsBox, lastQ = '';

  function bubble(cls, html) {
    var d = document.createElement('div');
    d.className = 'ek-b ' + cls;
    d.innerHTML = html;
    msgs.appendChild(d);
    msgs.scrollTop = msgs.scrollHeight;
    return d;
  }

  function post(url, data) {
    return fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) })
      .then(function (r) { return r.json(); });
  }

  function render(res, q) {
    var d = bubble('a', esc(res.text || ''));
    (res.orders || []).forEach(function (o) {
      var a = document.createElement('div');
      a.className = 'ek-o';
      a.innerHTML = '<b>' + esc(o.title) + '</b><span>' + esc(o.sub) + '</span>' + (o.prod ? '<span>' + esc(o.prod) + '</span>' : '');
      a.title = t('open');
      a.addEventListener('click', function () { location.href = '/orders?q=' + encodeURIComponent(o.search || ''); });
      d.appendChild(a);
    });
    if (res.confirm) {
      var b = document.createElement('button');
      b.className = 'ek-act';
      b.textContent = res.confirm.label;
      b.addEventListener('click', function () {
        b.disabled = true;
        post('/api/assistant/confirm', { token: res.confirm.token }).then(function (r) {
          bubble('a', esc(r.ok ? t('done') : t('expired')));
          if (r.ok && typeof window.loadOrders === 'function') try { window.loadOrders(window.currentPage || 1); } catch (e) {}
        });
      });
      d.appendChild(b);
    }
    if (res.teach) {
      var tb = document.createElement('button');
      tb.className = 'ek-act';
      tb.textContent = t('teach');
      tb.addEventListener('click', function () { tb.remove(); teachForm(d, q); });
      d.appendChild(tb);
    }
    if (res.log_id && !res.teach && res.intent !== 'help') {
      var fb = document.createElement('div');
      fb.className = 'ek-fb';
      ['👍', '👎'].forEach(function (s, i) {
        var bt = document.createElement('button');
        bt.textContent = s;
        bt.addEventListener('click', function () {
          fb.querySelectorAll('button').forEach(function (x) { x.classList.remove('on'); });
          bt.classList.add('on');
          post('/api/assistant/feedback', { log_id: res.log_id, good: i === 0 });
          if (i === 1 && !d.querySelector('.ek-teach')) teachForm(d, q);
        });
        fb.appendChild(bt);
      });
      d.appendChild(fb);
    }
    msgs.scrollTop = msgs.scrollHeight;
  }

  function teachForm(parent, q) {
    var f = document.createElement('div');
    f.className = 'ek-teach';
    f.innerHTML = '<input type="text" placeholder="' + esc(t('teachQ')) + '"><textarea rows="3" placeholder="' + esc(t('teachA')) + '"></textarea>'
      + '<button class="ek-act" type="button">' + esc(t('save')) + '</button>';
    f.querySelector('input').value = q || '';
    f.querySelector('button').addEventListener('click', function () {
      var qq = f.querySelector('input').value.trim(), aa = f.querySelector('textarea').value.trim();
      if (!qq || !aa) return;
      post('/api/assistant/teach', { question: qq, answer: aa }).then(function (r) {
        f.remove();
        bubble('a', esc(r.ok ? t('saved') : t('err')));
      });
    });
    parent.appendChild(f);
    f.querySelector('textarea').focus();
  }

  function send(q) {
    q = (q || input.value).trim();
    if (!q) return;
    input.value = '';
    lastQ = q;
    chipsBox.style.display = 'none';
    bubble('u', esc(q));
    var wait = bubble('a', '…');
    post('/api/assistant/ask', { q: q, lang: lang() }).then(function (res) {
      wait.remove();
      render(res, q);
    }).catch(function () { wait.textContent = t('err'); });
  }

  function chips() {
    chipsBox.innerHTML = '';
    t('chips').forEach(function (c) {
      var b = document.createElement('button');
      b.className = 'ek-chip';
      b.type = 'button';
      b.textContent = c;
      b.addEventListener('click', function () { send(c); });
      chipsBox.appendChild(b);
    });
  }

  function init() {
    if (!document.querySelector('.nav-user')) return; // только для вошедших
    var st = document.createElement('style');
    st.textContent = css;
    document.head.appendChild(st);

    var fab = document.createElement('button');
    fab.className = 'ek-as-fab';
    fab.type = 'button';
    fab.setAttribute('data-help', lang() === 'ru'
      ? 'Помощник: спроси обычными словами — сколько заказов, где заказ, вес товара, доставка. Если не знает — можно научить.'
      : 'Asystent: zapytaj zwykłymi słowami — ile zamówień, gdzie zamówienie, waga produktu, dostawa. Jeśli nie wie — możesz go nauczyć.');
    fab.textContent = '💬 ' + t('title');
    document.body.appendChild(fab);

    box = document.createElement('div');
    box.className = 'ek-as';
    box.innerHTML = '<div class="ek-as-h"><span>💬 ' + esc(t('title')) + '</span><button type="button" aria-label="close">×</button></div>'
      + '<div class="ek-as-m"></div><div class="ek-chips"></div>'
      + '<form class="ek-as-f"><input type="text" autocomplete="off"><button type="submit">➤</button></form>';
    document.body.appendChild(box);
    msgs = box.querySelector('.ek-as-m');
    input = box.querySelector('input');
    chipsBox = box.querySelector('.ek-chips');
    input.placeholder = t('ph');
    chips();
    bubble('a', esc(t('hello')));

    fab.addEventListener('click', function () {
      box.classList.toggle('open');
      if (box.classList.contains('open')) input.focus();
    });
    box.querySelector('.ek-as-h button').addEventListener('click', function () { box.classList.remove('open'); });
    box.querySelector('form').addEventListener('submit', function (e) { e.preventDefault(); send(); });
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && box.classList.contains('open') && document.activeElement === input) box.classList.remove('open');
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
