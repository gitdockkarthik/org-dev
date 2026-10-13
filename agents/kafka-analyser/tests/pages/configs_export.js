// Export CSV button on the alert rules card: request URL (portal proxy, auth
// headers, no filter parameters), the download (blob URL, file name from
// Content-Disposition or built in UTC), the "Exporting..." state, the error
// message on failure, and that the existing layout is kept.
const { TEAMS_HTML } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');
const clusters = [{ id: 4, name: 'c4', enabled: true }];
const configs = [
  { id: 3, name: 'CLOSE_WAIT spike', alert_type: 'close_wait_spike', cluster_id: null, cluster_name: 'All clusters', severity: 'warning', threshold: 1,
    config: { threshold: 1 }, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, enabled: true, email_enabled: false, open_trigger_count: 0, min_rf: null },
  { id: 20, name: 'URP - c4', alert_type: 'cluster.urp_total', cluster_id: 4, cluster_name: 'c4', severity: 'warning', threshold: 1,
    config: { mode: 'simple', warning: 1, critical: 5 }, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, enabled: false, email_enabled: false, open_trigger_count: 0, min_rf: null },
];
const CSV = 'id,name\r\n3,CLOSE_WAIT spike\r\n';
const calls = [], downloads = [], revoked = [];
let exportReply = null;   // () => Promise<response>, set per case
const headersOf = (h = {}) => ({ get: k => h[Object.keys(h).find(x => x.toLowerCase() === k.toLowerCase())] ?? null });
const json = (body, status = 200) => ({ ok: status < 400, status, headers: headersOf({ 'content-type': 'application/json' }), json: async () => body });
const csv = (headers = {}) => ({ ok: true, status: 200, headers: headersOf({ 'content-type': 'text/csv; charset=utf-8', ...headers }),
                                  blob: async () => ({ csvText: CSV }), json: async () => { throw new Error('not json'); } });
const dom = new JSDOM(html, {
  runScripts: 'dangerously', url: 'http://localhost/',
  beforeParse(w) {
    w.Element.prototype.scrollIntoView = () => {};
    w.mountUserBadge = () => {}; w.fetchCurrentUser = async () => null;
    // Portal mode: requests go through the backend proxy with the portal's auth headers.
    w._isPortalEnv = true; w.BACKEND_URL = 'http://backend'; w.portalReady = Promise.resolve();
    w._authHeaders = () => ({ 'X-API-Key': 'portal-key' });
    w.showToast = () => {};
    w.URL.createObjectURL = blob => { downloads.push({ blob }); return 'blob:export-' + downloads.length; };
    w.URL.revokeObjectURL = url => revoked.push(url);
    w.HTMLAnchorElement.prototype.click = function () {
      const last = downloads[downloads.length - 1];
      Object.assign(last, { href: this.href, download: this.download, attached: w.document.body.contains(this) });
    };
    w.fetch = async (url, opts = {}) => {
      calls.push({ url, method: opts.method || 'GET', headers: opts.headers || {} });
      if (url.endsWith('/settings/clusters')) return json({ clusters });
      if (url.includes('/alerts/triggers')) return json({ triggers: [], total_count: 0 });
      if (url.includes('/alerts/notifications')) return json({ notifications: [], total_count: 0 });
      if (url.includes('/alerts/configs/export')) return exportReply();
      if (url.endsWith('/alerts/configs')) return json({ configs: JSON.parse(JSON.stringify(configs)) });
      return json({});
    };
  },
});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '  ' + extra : ''}`); };
const sel = (id, v) => { $(id).value = v; $(id).dispatchEvent(new w.Event('change')); };
const btn = () => $('btn-configs-export'), err = () => $('configs-export-error');
const exportCalls = () => calls.filter(c => c.url.includes('/alerts/configs/export'));
const NAME_RE = /^kafka-alert-rules-\d{8}-\d{4}\.csv$/;
const utcName = t => `kafka-alert-rules-${t.toISOString().slice(0, 10).replace(/-/g, '')}-${t.toISOString().slice(11, 13)}${t.toISOString().slice(14, 16)}.csv`;

(async () => {
  await flush();
  const b = btn(), head = b.closest('.card-head');
  check('button: "Export CSV", type button, secondary, enabled', b.textContent === 'Export CSV' && b.type === 'button'
        && b.classList.contains('btn-secondary') && !b.disabled, b.outerHTML);
  check('button sits in the Alert Rules card header, right before "+ New Alert Rule"',
        head && head.querySelector('.card-title').textContent === 'Alert Rules' && b.nextElementSibling === $('btn-new-alert'));
  check('filter row unchanged: Reset filters is still its last item, no export control in it',
        $('btn-configs-reset').parentElement.lastElementChild === $('btn-configs-reset') && !d.querySelector('.configs-filters #btn-configs-export'));
  check('error line hidden and empty at load', err().classList.contains('hidden') && err().textContent === '');
  check('no export request at load', exportCalls().length === 0);

  // 1. Success, file name from Content-Disposition (direct, non-proxied mode).
  let release;
  exportReply = () => new Promise(r => { release = () => r(csv({ 'Content-Disposition': 'attachment; filename="kafka-alert-rules-20261013-0915.csv"' })); });
  sel('configs-cluster-filter', '4'); sel('configs-state-filter', 'disabled');
  $('configs-name-search').value = 'urp'; $('configs-name-search').dispatchEvent(new w.Event('input'));
  b.click();
  await flush();
  check('while waiting: "Exporting..." and disabled', b.textContent === 'Exporting...' && b.disabled);
  const c = exportCalls()[0];
  check('one GET to the proxy URL, no query string even with filters set',
        exportCalls().length === 1 && c.method === 'GET' && c.url === 'http://backend/api/agents/kafka-analyser/proxy/alerts/configs/export', c && c.url);
  check('request carries the portal auth headers', c.headers['X-API-Key'] === 'portal-key', JSON.stringify(c.headers));
  release(); await flush();
  const dl = downloads[0];
  check('downloaded the response body through a blob URL', downloads.length === 1 && dl.blob.csvText === CSV && dl.href === 'blob:export-1');
  check('file name taken from Content-Disposition', dl.download === 'kafka-alert-rules-20261013-0915.csv', dl.download);
  check('link was in the document when clicked and removed afterwards', dl.attached && !d.querySelector('a[href^="blob:"]'));
  check('button back to "Export CSV", enabled; no error', b.textContent === 'Export CSV' && !b.disabled && err().classList.contains('hidden'));
  check('blob URL revoked', revoked.includes('blob:export-1'), JSON.stringify(revoked));
  check('filters untouched by the export', $('configs-cluster-filter').value === '4' && $('configs-state-filter').value === 'disabled'
        && $('configs-name-search').value === 'urp');

  // 2. Success through the portal proxy, which drops Content-Disposition: name built in UTC.
  exportReply = async () => csv();
  const t0 = new Date(); b.click(); await flush(); const t1 = new Date();
  const name = downloads[1] && downloads[1].download;
  check('no Content-Disposition: name kafka-alert-rules-YYYYMMDD-HHMM.csv in UTC', NAME_RE.test(name || '') && [utcName(t0), utcName(t1)].includes(name), name);

  // 3. Failure with a JSON detail (503 timeout): message shown, no download, button restored.
  exportReply = async () => json({ detail: 'Export: the query took longer than 5s; try again' }, 503);
  b.click(); await flush();
  check('503: error line shows the server detail', !err().classList.contains('hidden')
        && err().textContent === 'Export failed: Export: the query took longer than 5s; try again', err().textContent);
  check('503: no download, button back to "Export CSV" and enabled', downloads.length === 2 && b.textContent === 'Export CSV' && !b.disabled);

  // 4. Failure without a JSON body (proxy 502 HTML) and a network error.
  exportReply = async () => ({ ok: false, status: 502, headers: headersOf(), json: async () => { throw new Error('html'); } });
  b.click(); await flush();
  check('502 without JSON: "Export failed: HTTP 502"', err().textContent === 'Export failed: HTTP 502', err().textContent);
  exportReply = async () => { throw new TypeError('Failed to fetch'); };
  b.click(); await flush();
  check('network error: message shown, button restored', err().textContent === 'Export failed: Failed to fetch' && !b.disabled && b.textContent === 'Export CSV');

  // 5. A later success clears the error.
  exportReply = async () => csv();
  b.click(); await flush();
  check('next success hides and clears the error', err().classList.contains('hidden') && err().textContent === '' && downloads.length === 3);
  check('rules table still rendered (export does not reload or change rules)',
        calls.filter(x => x.url.endsWith('/alerts/configs')).length === 1 && calls.every(x => x.method === 'GET'));

  // 6. Error text is not parsed as HTML.
  exportReply = async () => json({ detail: '<img src=x onerror=alert(1)>' }, 500);
  b.click(); await flush();
  check('error detail is shown as text', !err().querySelector('img') && err().textContent.includes('<img'), err().innerHTML);

  console.log(fails ? `${fails} FAILED` : 'all passed');
  process.exit(fails ? 1 : 0);
})();
