const { TEAMS_HTML, SETTINGS_HTML } = require('./_page');
// Step 3b portal stubs: runs the real inline <script> blocks of settings.html
// and teams.html against a minimal DOM built from each page's own elements.
const fs = require('fs');
const vm = require('vm');

let pass = 0, fail = 0;
function check(name, cond, extra = '') {
  if (cond) pass++; else fail++;
  console.log(`${cond ? 'PASS' : 'FAIL'} ${name}${extra ? `  [${extra}]` : ''}`);
}
const tick = () => new Promise(r => setImmediate(r));
const settle = async (n = 20) => { for (let i = 0; i < n; i++) await tick(); };
const rejections = [];
process.on('unhandledRejection', e => rejections.push(String(e && e.message)));

// ── minimal DOM ───────────────────────────────────────────────────────────
class ClassList {
  constructor(el) { this.el = el; }
  _set() { return new Set((this.el.className || '').split(/\s+/).filter(Boolean)); }
  add(...c) { const s = this._set(); c.forEach(x => s.add(x)); this.el.className = [...s].join(' '); }
  remove(...c) { const s = this._set(); c.forEach(x => s.delete(x)); this.el.className = [...s].join(' '); }
  toggle(c, force) { const has = this._set().has(c); const on = force === undefined ? !has : force; on ? this.add(c) : this.remove(c); return on; }
  contains(c) { return this._set().has(c); }
}
function parseAttrs(s) {
  const a = {};
  for (const m of s.matchAll(/([\w-]+)(?:\s*=\s*"([^"]*)")?/g)) a[m[1]] = m[2] ?? '';
  return a;
}
const unescape = s => s.replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
class El {
  constructor(tag = 'div', attrs = {}) {
    this.tagName = tag.toUpperCase(); this.id = attrs.id || ''; this.type = attrs.type || '';
    this.value = attrs.value || ''; this.checked = 'checked' in attrs; this.disabled = 'disabled' in attrs;
    this.placeholder = attrs.placeholder || ''; this.className = attrs.class || ''; this.title = attrs.title || '';
    this.dataset = {};
    for (const [k, v] of Object.entries(attrs)) if (k.startsWith('data-')) this.dataset[k.slice(5).replace(/-(\w)/g, (_, c) => c.toUpperCase())] = v;
    this.textContent = ''; this._innerHTML = ''; this._children = [];
    this.style = {}; this.classList = new ClassList(this); this.listeners = {}; this.options = [];
  }
  get innerHTML() { return this._innerHTML; }
  set innerHTML(v) {
    // Parse rendered inputs/buttons so listeners bound via querySelectorAll stick.
    this._innerHTML = v;
    this._children = [...String(v).matchAll(/<(input|button|select|span|label)\s([^>]*)>/g)].map(m => new El(m[1], parseAttrs(m[2])));
    // The label wrapping each toggle carries the tooltip; attach it to the input that follows.
    let lastTitle = null;
    for (const m of String(v).matchAll(/<(label|input)\s([^>]*)>/g)) {
      const a = parseAttrs(m[2]);
      if (m[1] === 'label') lastTitle = a.title ?? null;
      else { const el = this._children.find(c => c.tagName === 'INPUT' && c.dataset.id === a['data-id'] && c.className === a.class && !c._titled); if (el) { el.wrapTitle = lastTitle == null ? null : unescape(lastTitle); el._titled = true; } }
    }
  }
  addEventListener(ev, fn) { (this.listeners[ev] ||= []).push(fn); }
  async fire(ev) { await Promise.all((this.listeners[ev] || []).map(fn => fn.call(this, { target: this, preventDefault() {} }))); }
  click() { return this.fire('click'); }
  closest() { return new El('div'); }
  querySelector() { return new El('div'); }
  querySelectorAll(sel) {
    const cls = sel.startsWith('.') ? sel.slice(1) : null;
    return cls ? this._children.filter(c => c.classList.contains(cls)) : [];
  }
  appendChild(c) { return c; } append() {} prepend() {} remove() {} setAttribute(k, v) { this[k] = v; } getAttribute(k) { return this[k] ?? null; }
  removeAttribute() {} focus() {} scrollIntoView() {} insertAdjacentHTML() {}
}
function buildDom(html) {
  const els = {};
  for (const m of html.matchAll(/<(\w+)\s([^>]*\bid="[^"]+"[^>]*)>/g)) {
    const attrs = parseAttrs(m[2]);
    els[attrs.id] = new El(m[1], attrs);
  }
  // <select>: default value = first <option>.
  for (const m of html.matchAll(/<select\s[^>]*\bid="([^"]+)"[^>]*>([\s\S]*?)<\/select>/g)) {
    const opts = [...m[2].matchAll(/<option\s+value="([^"]*)"[^>]*>([^<]*)<\/option>/g)].map(o => ({ value: o[1], text: o[2] }));
    els[m[1]].options = opts;
    if (opts.length) els[m[1]].value = opts[0].value;
  }
  const missing = new Set();
  const document = {
    getElementById: id => els[id] || (missing.add(id), null),
    querySelectorAll: () => [], querySelector: () => null,
    createElement: tag => new El(tag), body: new El('body'), addEventListener() {},
  };
  return { els, document, missing };
}
function response(status, body) {
  return {
    ok: status >= 200 && status < 300, status,
    json: async () => (typeof body === 'string' ? JSON.parse(body) : body),
    text: async () => (typeof body === 'string' ? body : JSON.stringify(body)),
  };
}
function runPage(file, fetchImpl) {
  const html = fs.readFileSync(file, 'utf8');
  const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]);
  const dom = buildDom(html);
  const ctx = vm.createContext({
    window: {}, document: dom.document, fetch: fetchImpl, console,
    setTimeout: (fn, ms) => { const t = setTimeout(fn, ms); t.unref(); return t; },
    clearTimeout, setInterval: () => 0, clearInterval() {}, confirm: () => true, alert() {},
    localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
    location: { href: '', search: '', hash: '' }, navigator: {}, URLSearchParams, URL,
  });
  ctx.window = ctx;
  // Block 3 needs portal.js (mountUserBadge) and only handles nav/admin links.
  vm.runInContext(scripts[0], ctx, { filename: 'inline1.js' });
  vm.runInContext(scripts[1], ctx, { filename: 'inline2.js' });
  return { html, ...dom, ctx };
}

(async () => {
  // ═════ settings.html ═════
  console.log('=== settings.html: Send sample alert email ===');
  const calls = [];
  let handler = null;
  const sFetch = async (url, opts = {}) => {
    calls.push({ url, method: opts.method || 'GET', headers: opts.headers || {}, body: opts.body });
    if (handler && /\/settings\/email\//.test(url)) { const r = await handler(url, opts); if (r instanceof Error) throw r; return response(r.status, r.body); }
    if (url === '/settings') return response(200, { email_recipients: 'rules@operative.com', health_email_recipients: '' });
    if (url.startsWith('/settings/clusters')) return response(200, { clusters: [] });
    return response(200, {});
  };
  const S = runPage(SETTINGS_HTML, sFetch);
  await settle();
  const e = S.els, msg = e['email-test-msg'];
  const buttons = ['btn-test-email', 'btn-test-health-email', 'btn-test-alert-email'].map(id => e[id]);
  check('button and selector exist', !!e['btn-test-alert-email'] && !!e['email-test-kind']);
  check('selector options Fire/Escalate/Resolve -> fire/escalate/resolve, default fire',
    JSON.stringify(e['email-test-kind'].options) === JSON.stringify([{ value: 'fire', text: 'Fire' }, { value: 'escalate', text: 'Escalate' }, { value: 'resolve', text: 'Resolve' }])
    && e['email-test-kind'].value === 'fire');
  check('hint mentions all three buttons', /test email, sample health email, sample alert email/.test(S.html));

  e['email-test-to'].value = ' me@operative.com ';
  for (const kind of ['fire', 'escalate', 'resolve']) {
    e['email-test-kind'].value = kind;
    calls.length = 0;
    handler = async () => ({ status: 200, body: { ok: true, error: null } });
    await e['btn-test-alert-email'].click();
    const c = calls[0];
    check(`${kind}: POST /settings/email/test-alert {"to","kind"}`, c && c.url === '/settings/email/test-alert' && c.method === 'POST'
      && c.body === JSON.stringify({ to: 'me@operative.com', kind }) && c.headers['Content-Type'] === 'application/json', c && c.body);
    check(`${kind}: ok message`, msg.textContent === `✅ Sample alert email (${kind}) sent to me@operative.com` && msg.className === 'action-msg ok', msg.textContent);
  }

  e['email-test-kind'].value = 'escalate';
  let release; handler = () => new Promise(r => { release = r; });
  const p = e['btn-test-alert-email'].click();
  await tick();
  check('all three buttons disabled while running', buttons.every(b => b.disabled) && msg.textContent === 'Sending…');
  release({ status: 200, body: { ok: true, error: null } }); await p;
  check('all re-enabled after', buttons.every(b => !b.disabled));
  handler = () => new Promise(r => { release = r; });
  const p2 = e['btn-test-email'].click(); await tick();
  check('existing buttons also disable the new one', e['btn-test-alert-email'].disabled === true);
  release({ status: 200, body: { ok: true, error: null } }); await p2;

  const cases = [
    ['200 ok:false', { status: 200, body: { ok: false, error: 'connection refused' } }, '❌ Send failed: connection refused'],
    ['422', { status: 422, body: { detail: 'not configured: smtp_host' } }, '❌ not configured: smtp_host (save the SMTP settings first)'],
    ['429', { status: 429, body: { detail: 'test email rate limit: try again in 4s' } }, '❌ Too many test emails: wait 10 seconds and try again'],
    ['400 kind', { status: 400, body: { detail: 'kind must be fire, escalate or resolve' } }, '❌ kind must be fire, escalate or resolve'],
    ['400 domain', { status: 400, body: { detail: 'test emails can only be sent to @operative.com or @sintecmedia.com addresses' } }, '❌ test emails can only be sent to @operative.com or @sintecmedia.com addresses'],
    ['500 non-JSON', { status: 500, body: '<html>oops</html>' }, '❌ Failed (HTTP 500)'],
    ['network error', new Error('NetworkError'), '❌ Request failed: NetworkError'],
  ];
  for (const [name, r, expected] of cases) {
    handler = async () => r;
    await e['btn-test-alert-email'].click();
    check(`alert ${name} -> "${expected}"`, msg.textContent === expected && msg.className === 'action-msg err' && buttons.every(b => !b.disabled), msg.textContent);
  }
  e['email-test-to'].value = '';
  calls.length = 0;
  await e['btn-test-alert-email'].click();
  check('empty address -> message, no request', msg.textContent === 'Enter an address first' && !calls.length);

  e['email-test-to'].value = 'me@operative.com';
  handler = async () => ({ status: 200, body: { ok: true, error: null } });
  calls.length = 0; await e['btn-test-email'].click(); await e['btn-test-health-email'].click();
  check('existing buttons unchanged: body is {"to"} only', calls.map(c => `${c.url} ${c.body}`).join(' | ')
    === '/settings/email/test {"to":"me@operative.com"} | /settings/email/test-health {"to":"me@operative.com"}', calls.map(c => c.body).join(' | '));

  // ═════ teams.html ═════
  console.log('=== teams.html: Email toggle tooltip ===');
  const tCalls = [];
  const CONFIGS = [
    { id: 1, name: 'CPU high', alert_type: 'broker.cpu_pct', cluster_id: 1, cluster_name: 'prod-a', severity: 'warning', tiers: { warning: 80 },
      cooldown_minutes: 30, enabled: true, email_enabled: true, open_trigger_count: 0, send_resolve_card: true },
    { id: 2, name: 'Close wait', alert_type: 'close_wait_spike', cluster_id: 1, cluster_name: 'prod-a', severity: 'critical', threshold: 1,
      cooldown_minutes: 30, enabled: true, email_enabled: false, open_trigger_count: 1 },
  ];
  let putFails = false;
  const tFetch = async (url, opts = {}) => {
    tCalls.push({ url, method: opts.method || 'GET', body: opts.body });
    if (url === '/alerts/configs') return response(200, { configs: CONFIGS });
    if (/^\/alerts\/configs\/\d+$/.test(url) && opts.method === 'PUT') {
      if (putFails) return response(500, { detail: 'db down' });
      const id = +url.split('/').pop(); Object.assign(CONFIGS.find(c => c.id === id), JSON.parse(opts.body));
      return response(200, { config: CONFIGS.find(c => c.id === id) });
    }
    if (url.startsWith('/alerts/triggers')) return response(200, { triggers: [], total: 0 });
    if (url.startsWith('/settings/clusters')) return response(200, { clusters: [{ id: 1, name: 'prod-a' }] });
    return response(200, {});
  };
  const T = runPage(TEAMS_HTML, tFetch);
  await settle(40);
  const NEW = 'Sends an email to the Alert rule recipients (Settings) when this rule fires, escalates or resolves. Needs Email Escalation enabled and recipients set.';
  const toggles = () => T.els['configs-body'].querySelectorAll('.config-email-toggle');
  let tg = toggles();
  check('rules table rendered with two Email toggles', tg.length === 2, `${tg.length}; missing ids: ${[...T.missing].join(',')}`);
  const byId = id => toggles().find(t => t.dataset.id === String(id));
  check('email on -> "Enabled. <new text>"', byId(1) && byId(1).wrapTitle === 'Enabled. ' + NEW, byId(1) && byId(1).wrapTitle);
  check('email off -> "Disabled. <new text>"', byId(2) && byId(2).wrapTitle === 'Disabled. ' + NEW, byId(2) && byId(2).wrapTitle);
  check('checked state follows email_enabled', byId(1).checked === true && byId(2).checked === false);
  check('old "not yet configured" text gone from page', !/Email sending is not yet configured/.test(T.html));

  tCalls.length = 0;
  const t2 = byId(2); t2.checked = true; await t2.fire('change'); await settle(20);
  const put = tCalls.find(c => c.method === 'PUT');
  check('toggle behaviour unchanged: PUT {"email_enabled":true} then reload', put && put.url === '/alerts/configs/2' && put.body === '{"email_enabled":true}'
    && tCalls.some(c => c.url === '/alerts/configs' && c.method === 'GET'), put && put.body);
  check('after reload the tooltip flips to "Enabled."', byId(2).wrapTitle === 'Enabled. ' + NEW);
  putFails = true;
  const t1 = byId(1); t1.checked = false; await t1.fire('change'); await settle(20);
  check('failed PUT reverts the toggle and shows the error', t1.checked === true && t1.disabled === false
    && T.els['configs-msg'].textContent === '✗ Could not disable email for rule: db down', T.els['configs-msg'].textContent);

  console.log('   unhandled rejections:', JSON.stringify(rejections));
  console.log(`\n${pass}/${pass + fail} passed`);
  process.exit(fail ? 1 : 0);
})().catch(err => { console.error(err); process.exit(2); });
