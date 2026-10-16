// settings.html: the "Broker restart window (minutes)" number field (Alert Rules card). Loaded from
// GET /settings (missing = 30), sent by Save as broker_restart_window_minutes (a number); routes_settings
// validates 15 to 240 with a 422.
const { SETTINGS_HTML } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(SETTINGS_HTML, 'utf8');

let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '  ' + extra : ''}`); };
const flush = () => new Promise(r => setTimeout(r, 50));
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body, text: async () => JSON.stringify(body) });

async function load(cfg) {
  const posts = [];
  const dom = new JSDOM(html, { runScripts: 'dangerously', url: 'http://localhost/', beforeParse(w) {
    w.Element.prototype.scrollIntoView = () => {};
    w.mountUserBadge = () => {}; w.fetchCurrentUser = async () => null;
    w.fetch = async (url, opts = {}) => {
      if ((opts.method || 'GET') === 'POST') { posts.push({ url, body: JSON.parse(opts.body || '{}') }); return json({ ok: true }); }
      if (/\/settings$/.test(url)) return json(cfg);
      if (/\/settings\/clusters/.test(url)) return json({ clusters: [] });
      return json({});
    };
  }});
  await flush(); await flush();
  return { w: dom.window, $: id => dom.window.document.getElementById(id), posts };
}

(async () => {
  let p = await load({});
  const f = p.$('broker-restart-window');
  check('number field present with min 15, max 240, step 1, in the Alert Rules card of the Teams Connection tab',
    !!f && f.type === 'number' && f.min === '15' && f.max === '240' && f.step === '1' && f.closest('.card').querySelector('.card-section-title').textContent === 'Alert Rules'
    && !!f.closest('#panel-thresholds'));
  check('label text', p.$('broker-restart-window').closest('.field').querySelector('label').textContent === 'Broker restart window (minutes)');
  check('setting missing from GET /settings: 30', f.value === '30', f.value);
  p.$('btn-save-thresholds').click(); await flush();
  let post = p.posts.find(x => /\/settings$/.test(x.url));
  check('Save sends broker_restart_window_minutes 30 as a number', post && post.body.broker_restart_window_minutes === 30, JSON.stringify(post && post.body.broker_restart_window_minutes));

  p = await load({ broker_restart_window_minutes: 90 });
  check('setting 90 loaded', p.$('broker-restart-window').value === '90');
  p.$('broker-restart-window').value = '45';
  p.$('btn-save-thresholds').click(); await flush();
  post = p.posts.find(x => /\/settings$/.test(x.url));
  check('edited to 45: Save sends 45', post && post.body.broker_restart_window_minutes === 45, JSON.stringify(post && post.body.broker_restart_window_minutes));
  p.posts.length = 0;
  p.$('broker-restart-window').value = '';
  p.$('btn-save-thresholds').click(); await flush();
  post = p.posts.find(x => /\/settings$/.test(x.url));
  check('emptied: Save sends the default 30, never NaN or null', post && post.body.broker_restart_window_minutes === 30, JSON.stringify(post && post.body.broker_restart_window_minutes));
  console.log(fails ? `${fails} FAILED` : 'ALL PASS');
  process.exit(fails ? 1 : 0);
})();
