const { TEAMS_HTML, ROUTES_ALERTS } = require('./_page');
// SLO compliance rule type: the real teams.html in jsdom, fetch stubbed.
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');
const routesAlerts = fs.readFileSync(ROUTES_ALERTS, 'utf8');

const calls = [];
const clusters = [{ id: 4, name: 'c4', enabled: true }, { id: 8, name: 'c8', enabled: true }];
const base = { severity: 'warning', threshold: 1, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, enabled: true, email_enabled: false, open_trigger_count: 0, min_rf: null };
const configs = [
  { ...base, id: 60, name: 'SLO compliance - c4', alert_type: 'cluster.slo_compliance', cluster_id: 4, cluster_name: 'c4',
    config: { mode: 'simple', direction: 'below', warning: 75, critical: 60 } },
  { ...base, id: 61, name: 'SLO zero - c8', alert_type: 'cluster.slo_compliance', cluster_id: 8, cluster_name: 'c8',
    config: { mode: 'simple', direction: 'below', info: 99.5, warning: 75, critical: 0 } },
  { ...base, id: 62, name: 'Rate in - c4', alert_type: 'cluster.msg_rate_in', cluster_id: 4, cluster_name: 'c4',
    config: { mode: 'simple', direction: 'below', warning: 100, critical: 0 } },
];
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });

const dom = new JSDOM(html, {
  runScripts: 'dangerously',
  url: 'http://localhost/',
  beforeParse(w) {
    w.Element.prototype.scrollIntoView = () => {};
    w.mountUserBadge = () => {};
    w.fetchCurrentUser = async () => null;
    w.fetch = async (url, opts = {}) => {
      const method = opts.method || 'GET';
      if (url.endsWith('/settings/clusters')) return json({ clusters });
      if (url.includes('/alerts/triggers')) return json({ triggers: [], total_count: 0 });
      if (url.endsWith('/alerts/configs') && method === 'GET') return json({ configs });
      calls.push({ method, url, body: opts.body ? JSON.parse(opts.body) : null });
      if (url.includes('/dashboard/')) return json({ empty: true });
      return json({ config: { id: 100, cluster_name: 'c4' } });
    };
  },
});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0, passes = 0;
const check = (label, cond, extra = '') => { cond ? passes++ : fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '  ' + extra : ''}`); };
const setType = v => { $('af-alert-type').value = v; $('af-alert-type').dispatchEvent(new w.Event('change')); };
const setCluster = v => { $('af-cluster').value = v; $('af-cluster').dispatchEvent(new w.Event('change')); };
const newForm = (name, type) => { $('btn-new-alert').click(); $('af-name').value = name; if (type) setType(type); };
async function save() { const n = calls.length; $('btn-save-alert').click(); await flush(); return calls.slice(n).filter(c => c.url.includes('/alerts/')); }
const msg = () => $('alert-form-msg').textContent;
const shown = id => !$(id).classList.contains('hidden');
const setTiers = (i, wv, c) => { $('af-tier-info').value = i; $('af-tier-warning').value = wv; $('af-tier-critical').value = c; };
const rowFor = id => [...d.querySelectorAll('#configs-body tr')].find(tr => tr.querySelector(`button.config-edit[data-id="${id}"]`));
const SLO = 'cluster.slo_compliance';

(async () => {
  await flush();
  console.log('--- type list ---');
  const opts = [...$('af-alert-type').querySelectorAll('optgroup[label="Cluster"] option')];
  const vals = opts.map(o => o.value);
  check('SLO compliance under Cluster, after the lag types', JSON.stringify(vals.slice(-3)) === JSON.stringify(
    ['cluster.consumer_lag', 'cluster.connector_lag', SLO]), JSON.stringify(vals.slice(-3)));
  check('label "SLO compliance"', opts.at(-1).textContent === 'SLO compliance', opts.at(-1).textContent);
  check('label matches _TYPE_LABELS', /"cluster\.slo_compliance":\s*"SLO compliance"/.test(routesAlerts));
  check('only one SLO option in the select', $('af-alert-type').querySelectorAll(`option[value="${SLO}"]`).length === 1);

  console.log('--- hints, every type ---');
  const HINT_IDS = [...d.querySelectorAll('#af-tiers-field .hint')].map(h => h.id);
  const allTypes = [...$('af-alert-type').querySelectorAll('option')].map(o => o.value);
  newForm('h');
  let sloOnly = true, belowOk = true;
  for (const v of allTypes) {
    setType(v);
    if (shown('af-tiers-slo-hint') !== (v === SLO)) { sloOnly = false; console.log('   slo hint wrong for', v); }
    const wantBelow = v === 'cluster.msg_rate_in';
    if (shown('af-tiers-below-hint') !== wantBelow) { belowOk = false; console.log('   below hint wrong for', v); }
  }
  check(`SLO hint shown only for SLO (${allTypes.length} types checked)`, sloOnly);
  check('msg_rate_in hint still shown only for msg_rate_in', belowOk);
  setType(SLO);
  const visibleHints = HINT_IDS.filter(shown);
  check('SLO: only the SLO hint visible in the tiers block', JSON.stringify(visibleHints) === JSON.stringify(['af-tiers-slo-hint']), JSON.stringify(visibleHints));
  check('hint text exact', $('af-tiers-slo-hint').textContent === "Fires when the cluster's overall SLO compliance for the latest hour falls BELOW a tier. Enter tiers from high to low (warning > critical). The figure is calculated hourly, and two hourly readings in a row are needed, so a real drop takes about two hours to fire. See the SLI/SLO tab for what is below target.");
  check('tiers field shown, severity/threshold/min RF/lag hidden', shown('af-tiers-field') && !shown('af-min-rf-field') && !shown('af-lag-field')
    && $('af-severity').closest('.field').classList.contains('hidden') && $('af-threshold').closest('.field').classList.contains('hidden'));
  check('info tier shown (as for msg_rate_in)', !$('af-tier-info').closest('div').classList.contains('hidden') && $('af-tier-info').offsetParent !== undefined);
  check('unit "(%)" on tier inputs', [...d.querySelectorAll('.af-tier-unit')].every(e => e.textContent === ' (%)'));
  check('resolve card option shown', !$('af-resolve-card').closest('.field').classList.contains('hidden'));

  console.log('--- save (new) ---');
  setCluster('4'); setTiers('', '75', '60');
  let sent = await save();
  const post = sent.find(c => c.method === 'POST');
  check('POST sent', !!post);
  check('tiers sent as numbers {warning:75, critical:60}', post && JSON.stringify(post.body.tiers) === '{"warning":75,"critical":60}', post && JSON.stringify(post.body.tiers));
  check('no min_rf in payload', post && !('min_rf' in post.body));
  check('no lag fields in payload', post && !('custom' in post.body) && !('blacklist' in post.body) && !('reminder_hours' in post.body));
  check('alert_type and cluster sent', post && post.body.alert_type === SLO && post.body.cluster_id === 4);
  check('send_resolve_card sent', post && typeof post.body.send_resolve_card === 'boolean');

  console.log('--- validation ---');
  for (const [i, wv, c, want, label] of [
    ['', '100.5', '60', 'Warning at: Must be between 0 and 100', 'warning 100.5'],
    ['', '75', '-1', 'Critical at: Must be between 0 and 100', 'critical -1'],
    ['101', '75', '60', 'Info at: Must be between 0 and 100', 'info 101'],
    ['', 'abc', '', null, 'non-number'],
  ]) {
    newForm('v', SLO); setCluster('4'); setTiers(i, wv, c);
    sent = await save();
    const m = msg();
    check(`${label} rejected, nothing sent`, sent.length === 0 && (want ? m.includes(want) : m.startsWith('✗')), JSON.stringify(m));
  }
  newForm('z', SLO); setCluster('4'); setTiers('100', '50', '0');
  sent = await save();
  const z = sent.find(c => c.method === 'POST');
  check('0 accepted, 100 accepted: {info:100, warning:50, critical:0}', z && JSON.stringify(z.body.tiers) === '{"info":100,"warning":50,"critical":0}', z && JSON.stringify(z.body.tiers));
  newForm('e', SLO); setCluster('4'); setTiers('', '', '');
  sent = await save();
  check('no tier set rejected', sent.length === 0 && msg().includes('Set at least one'), msg());
  // msg_rate_in unchanged: no 100 cap
  newForm('r', 'cluster.msg_rate_in'); setCluster('4'); setTiers('', '500', '0');
  sent = await save();
  const r = sent.find(c => c.method === 'POST');
  check('msg_rate_in still accepts 500 and 0 (no 100 cap)', r && JSON.stringify(r.body.tiers) === '{"warning":500,"critical":0}', r && JSON.stringify(r.body.tiers));
  newForm('r2', 'cluster.msg_rate_in'); setCluster('4'); setTiers('', '-1', '');
  sent = await save();
  check('msg_rate_in -1 keeps its old message', sent.length === 0 && msg().includes('Warning at must be a number of 0 or more'), msg());
  newForm('cpu', 'broker.cpu_pct'); setCluster('4'); setTiers('', '150', '');
  sent = await save();
  check('broker.cpu_pct (non-pct type) still accepts 150', sent.some(c => c.method === 'POST' && c.body.tiers.warning === 150));

  console.log('--- table ---');
  const cell = id => rowFor(id) && [...rowFor(id).querySelectorAll('td')].map(td => td.textContent).find(t => /warn|crit/.test(t));
  check('row 60: "warn 75 · crit 60 %"', cell(60) === 'warn 75 · crit 60 %', JSON.stringify(cell(60)));
  check('row 61: crit 0 shown, not missing', cell(61) === 'info 99.5 · warn 75 · crit 0 %', JSON.stringify(cell(61)));
  check('row 60 type cell says SLO compliance', rowFor(60).textContent.includes('SLO compliance'));
  check('row 62 msg_rate_in unchanged', cell(62) === 'warn 100 · crit 0 msgs/sec', JSON.stringify(cell(62)));

  console.log('--- edit ---');
  rowFor(61).querySelector('button.config-edit').click(); await flush();
  check('type fixed to SLO and disabled', $('af-alert-type').value === SLO && $('af-alert-type').disabled);
  check('inputs filled 99.5 / 75 / 0', $('af-tier-info').value === '99.5' && $('af-tier-warning').value === '75' && $('af-tier-critical').value === '0',
    [$('af-tier-info').value, $('af-tier-warning').value, $('af-tier-critical').value].join('/'));
  check('SLO hint shown in edit', shown('af-tiers-slo-hint') && !shown('af-tiers-below-hint'));
  sent = await save();
  const put = sent.find(c => c.method === 'PUT');
  check('PUT /alerts/configs/61 with the same numbers', put && put.url.endsWith('/alerts/configs/61') && JSON.stringify(put.body.tiers) === '{"info":99.5,"warning":75,"critical":0}', put && JSON.stringify(put.body.tiers));
  check('edit save has no min_rf', put && !('min_rf' in put.body));
  rowFor(60).querySelector('button.config-edit').click(); await flush();
  check('edit 60: info empty, 75 / 60', $('af-tier-info').value === '' && $('af-tier-warning').value === '75' && $('af-tier-critical').value === '60');
  sent = await save();
  const put60 = sent.find(c => c.method === 'PUT');
  check('PUT 60 saves {warning:75, critical:60}', put60 && JSON.stringify(put60.body.tiers) === '{"warning":75,"critical":60}', put60 && JSON.stringify(put60.body.tiers));

  console.log(`\n${passes} passed, ${fails} failed`);
  process.exit(fails ? 1 : 0);
})().catch(e => { console.error(e); process.exit(2); });
