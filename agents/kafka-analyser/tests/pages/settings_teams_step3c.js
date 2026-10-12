const { TEAMS_HTML, SETTINGS_HTML } = require('./_page');
// Step 3c portal stubs (harness copied from test_step3b_pages.js).
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
  console.log('=== settings.html ===');
  const html = fs.readFileSync(SETTINGS_HTML, 'utf8');
  check('new Alert rule recipients hint', html.includes('<div class="hint">Recipients of alert rule emails. A rule sends email only when its Email toggle is on (Alert Rules) and Email Escalation is enabled above.</div>'));
  check('old "(not wired yet)" hint gone', !html.includes('not wired yet'));
  check('#sync-status still absent from the page (the case being fixed)', !/id="sync-status"/.test(html));

  const CFG = {
    source_type: 'synthetic', teams_enabled: true, teams_webhook_url: 'https://teams.example/hook',
    email_enabled: true, smtp_host: 'mail.operative.com', smtp_port: 25, smtp_username: '',
    smtp_password_configured: false, smtp_from_address: 'ai_agent_alerts@sintecmedia.com',
    email_recipients: 'rules@operative.com', health_email_recipients: 'owner@operative.com',
    lag_threshold: 12345, heap_threshold_pct: 77,
  };
  let calls = [], mode = 'ok';
  const sFetch = async (url, opts = {}) => {
    calls.push(url);
    if (url === '/settings') {
      if (mode === 'throw') throw new Error('backend down');
      return response(200, CFG);
    }
    if (url.startsWith('/settings/clusters')) return response(200, { clusters: [{ id: 1, name: 'prod-a', enabled: true }] });
    if (url.startsWith('/dashboard/overview')) return response(200, { cluster: { status: 'healthy' } });
    return response(200, {});
  };
  rejections.length = 0;
  const S = runPage(SETTINGS_HTML, sFetch);
  await settle(40);
  const e = S.els;
  check('loadSettings completes with no unhandled error', rejections.length === 0, JSON.stringify(rejections));
  check('all email/SMTP/Teams fields load', e['teams-enabled'].checked === true && e['teams-webhook-url'].value === 'https://teams.example/hook'
    && e['email-enabled'].checked === true && e['smtp-host'].value === 'mail.operative.com' && String(e['smtp-port'].value) === '25'
    && e['smtp-username'].value === '' && e['smtp-from-address'].value === 'ai_agent_alerts@sintecmedia.com'
    && e['email-recipients'].value === 'rules@operative.com' && e['health-email-recipients'].value === 'owner@operative.com');
  check('per-cluster status not run (no /dashboard/overview), same as before', !calls.some(u => u.startsWith('/dashboard/overview')), calls.join(','));

  // With a #sync-status element present, the old path runs as before.
  console.log('--- with #sync-status present ---');
  const html2 = html.replace('<div id="email-test-msg"', '<div id="sync-status"></div><div id="email-test-msg"').replace('<span id="email-test-msg"', '<div id="sync-status"></div><span id="email-test-msg"');
  const tmp = require('path').join(require('os').tmpdir(), `settings_with_sync_${process.pid}.html`);
  fs.writeFileSync(tmp, html2);
  calls = []; rejections.length = 0;
  const S2 = runPage(tmp, sFetch);
  S2.els['sync-status'].innerHTML = 'stale';
  await settle(60);
  check('element present: cleared, and per-cluster status requested', S2.els['sync-status'].innerHTML === ''
    && calls.includes('/settings/clusters') && calls.some(u => u.startsWith('/dashboard/overview')), calls.join(','));

  console.log('--- error path (GET /settings fails) ---');
  mode = 'throw'; rejections.length = 0;
  runPage(SETTINGS_HTML, sFetch);
  await settle(40);
  check('sync-status write is guarded; remaining error is the missing #last-synced-label (left as is)',
    rejections.length === 1 && rejections[0] === "Cannot set properties of null (setting 'textContent')", JSON.stringify(rejections));
  mode = 'throw'; rejections.length = 0;
  const S3 = runPage(tmp, sFetch);
  await settle(40);
  check('element present: error message written to #sync-status as before', S3.els['sync-status'].innerHTML.includes('Failed to load settings: backend down'));
  fs.unlinkSync(tmp);

  // ═════ teams.html ═════
  console.log('=== teams.html ===');
  const thtml = fs.readFileSync(TEAMS_HTML, 'utf8');
  check('description reads "(Teams and Email)"', thtml.includes('<p>Configure alert rules and scheduled reminders across notification channels (Teams and Email)</p>'));
  check('old "once configured" text gone', !thtml.includes('once configured'));
  check('no opacity left on any toggle label', !/opacity:0\.65/.test(thtml));

  const CONFIGS = [
    { id: 1, name: 'CPU high', alert_type: 'broker.cpu_pct', cluster_id: 1, cluster_name: 'prod-a', severity: 'warning', tiers: { warning: 80 },
      cooldown_minutes: 30, enabled: true, email_enabled: true, open_trigger_count: 0, send_resolve_card: true },
    { id: 2, name: 'Close wait', alert_type: 'close_wait_spike', cluster_id: 1, cluster_name: 'prod-a', severity: 'critical', threshold: 1,
      cooldown_minutes: 30, enabled: true, email_enabled: false, open_trigger_count: 1 },
  ];
  const tCalls = [];
  const tFetch = async (url, opts = {}) => {
    tCalls.push({ url, method: opts.method || 'GET', body: opts.body });
    if (url === '/alerts/configs') return response(200, { configs: CONFIGS });
    if (/^\/alerts\/configs\/\d+$/.test(url) && opts.method === 'PUT') {
      const id = +url.split('/').pop(); Object.assign(CONFIGS.find(c => c.id === id), JSON.parse(opts.body));
      return response(200, {});
    }
    if (url.startsWith('/alerts/triggers')) return response(200, { triggers: [], total: 0 });
    if (url.startsWith('/settings/clusters')) return response(200, { clusters: [{ id: 1, name: 'prod-a' }] });
    return response(200, {});
  };
  rejections.length = 0;
  const T = runPage(TEAMS_HTML, tFetch);
  await settle(40);
  const body = T.els['configs-body'];
  const NEW = 'Sends an email to the Alert rule recipients (Settings) when this rule fires, escalates or resolves. Needs Email Escalation enabled and recipients set.';
  const labels = [...body.innerHTML.matchAll(/<label class="toggle-wrap"[^>]*>\s*<input[^>]*config-email-toggle[^>]*>/g)].map(m => m[0]);
  check('rendered Email toggle labels have no style attribute', labels.length === 2 && labels.every(l => !/<label[^>]*style=/.test(l)), labels.length);
  const byId = id => body.querySelectorAll('.config-email-toggle').find(t => t.dataset.id === String(id));
  check('same title (Enabled./Disabled. + text)', byId(1).wrapTitle === 'Enabled. ' + NEW && byId(2).wrapTitle === 'Disabled. ' + NEW);
  check('same classes on label and input', labels.every(l => l.includes('<label class="toggle-wrap" title=') && l.includes('class="toggle-input config-email-toggle"')));
  tCalls.length = 0;
  const t2 = byId(2); t2.checked = true; await t2.fire('change'); await settle(20);
  const put = tCalls.find(c => c.method === 'PUT');
  check('toggle still PUTs {"email_enabled":true} and reloads', put && put.url === '/alerts/configs/2' && put.body === '{"email_enabled":true}'
    && tCalls.some(c => c.url === '/alerts/configs' && c.method === 'GET'));
  check('teams page: no unhandled errors', rejections.length === 0, JSON.stringify(rejections));

  console.log(`\n${pass}/${pass + fail} passed`);
  process.exit(fail ? 1 : 0);
})().catch(err => { console.error(err); process.exit(2); });
