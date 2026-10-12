const { TEAMS_HTML, alertTypes, consecutive, tierHintProblems } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');

const calls = [];
const clusters = [{ id: 4, name: 'c4', enabled: true }, { id: 8, name: 'c8', enabled: true }, { id: 9, name: 'c9', enabled: true }];
const base = { severity: 'warning', threshold: 1, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, enabled: true, email_enabled: false, open_trigger_count: 0 };
const configs = [
  { ...base, id: 20, name: 'Broker CPU % - c4', alert_type: 'broker.cpu_pct', cluster_id: 4, cluster_name: 'c4', config: { mode: 'simple', warning: 70, critical: 85 }, min_rf: null },
  { ...base, id: 30, name: 'RF - c4', alert_type: 'cluster.rf_below_min', cluster_id: 4, cluster_name: 'c4', config: { mode: 'simple', warning: 5, critical: 20, min_rf: 3 }, min_rf: 3 },
  { ...base, id: 31, name: 'URP - c4', alert_type: 'cluster.urp_total', cluster_id: 4, cluster_name: 'c4', config: { mode: 'simple', info: 1, critical: 100 }, min_rf: null },
  { ...base, id: 34, name: 'Rate - c8', alert_type: 'cluster.msg_rate_in', cluster_id: 8, cluster_name: 'c8', config: { mode: 'simple', direction: 'below', info: 1, warning: 0.5, critical: 0 }, min_rf: null },
  { ...base, id: 32, name: 'RF <evil> - c8', alert_type: 'cluster.rf_below_min', cluster_id: 8, cluster_name: 'c8', config: { mode: 'simple', warning: 1, min_rf: '<b>2</b>' }, min_rf: '<b>2</b>' },
];
let nextResponse = null;  // one-shot override for the next non-GET request
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
      if (nextResponse) { const r = nextResponse; nextResponse = null; return r; }
      if (url.endsWith('/alerts/configs/copy')) return json({ created: [{ id: 101 }, { id: 102 }], skipped: [] });
      return json({ config: { id: 100, cluster_name: 'c' + (calls.at(-1).body || {}).cluster_id } });
    };
  },
});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '  ' + extra : ''}`); };
const setType = v => { $('af-alert-type').value = v; $('af-alert-type').dispatchEvent(new w.Event('change')); };
const setCluster = v => { $('af-cluster').value = v; $('af-cluster').dispatchEvent(new w.Event('change')); };
const newForm = (name, type) => { $('btn-new-alert').click(); $('af-name').value = name; if (type) setType(type); };
async function save() { const n = calls.length; $('btn-save-alert').click(); await flush(); return calls.slice(n); }
const msg = () => $('alert-form-msg').textContent;
const minRfShown = () => !$('af-min-rf-field').classList.contains('hidden');
const rowCell = id => [...d.querySelectorAll('#configs-body tr')].find(tr => tr.querySelector(`button.config-edit[data-id="${id}"]`));

(async () => {
  await flush();
  const grp = $('af-alert-type').querySelector('optgroup[label="Cluster"]');
  const opts = [...grp.querySelectorAll('option')].map(o => `${o.value}=${o.textContent}`);
  check('message rate in under "Cluster" with its label', opts.includes('cluster.msg_rate_in=Message rate in'), JSON.stringify(opts));
  const shown = id => !$(id).classList.contains('hidden');
  newForm('vis');
  // The below hint belongs to msg_rate_in; the above hint is hidden for every below type
  // (ALERT_TYPES below: true) and shown for the others (_page.tierHintProblems).
  const bad = tierHintProblems(w, setType, { 'af-tiers-below-hint': 'cluster.msg_rate_in' });
  check('below hint only for msg_rate_in; above hint hidden for below types, kept for every other type', bad.length === 0
        && alertTypes(w).find(t => t.value === 'cluster.msg_rate_in').below === true, bad.join(', '));
  setType('cluster.msg_rate_in');
  check('below hint text', $('af-tiers-below-hint').textContent === "Fires when the cluster's inflow falls BELOW a tier. Enter tiers from high to low (info > warning > critical); 0 means no messages at all. Needs two consecutive inflow runs (about 10 minutes apart), so a real drop takes about 20 minutes to fire.");
  check('tiers shown, min RF hidden, unit "(msgs/sec)", inputs min 0 step any', shown('af-tiers-field') && !shown('af-min-rf-field')
        && d.querySelector('.af-tier-unit').textContent === ' (msgs/sec)' && $('af-tier-critical').min === '0' && $('af-tier-critical').step === 'any');
  $('btn-cancel-alert').click();

  newForm('Rate - c9', 'cluster.msg_rate_in'); setCluster('9');
  $('af-tier-info').value = '1'; $('af-tier-warning').value = '0.5'; $('af-tier-critical').value = '0'; $('af-min-rf').value = '3';
  let sent = await save(), b = sent[0] && sent[0].body;
  check('create sends 1 / 0.5 / 0 as numbers, no min_rf', sent.length === 1 && JSON.stringify(b.tiers) === '{"info":1,"warning":0.5,"critical":0}'
        && typeof b.tiers.critical === 'number' && !('min_rf' in b) && b.alert_type === 'cluster.msg_rate_in', JSON.stringify(b));
  newForm('Rate neg', 'cluster.msg_rate_in'); setCluster('9'); $('af-tier-critical').value = '-1';
  sent = await save();
  check('negative rejected for msg_rate_in', sent.length === 0 && msg() === '✗ Critical at must be a number of 0 or more', msg());
  for (const t of ['broker.cpu_pct', 'cluster.urp_total', 'cluster.leader_skew']) {
    newForm('zero ' + t, t); setCluster('9'); $('af-tier-warning').value = '0'; if (t === 'cluster.rf_below_min') $('af-min-rf').value = '3';
    sent = await save();
    check(`0 still rejected for ${t}`, sent.length === 0 && msg() === '✗ Warning at must be a number greater than 0', msg());
  }
  newForm('zero rf', 'cluster.rf_below_min'); setCluster('9'); $('af-tier-warning').value = '0'; $('af-min-rf').value = '3';
  sent = await save();
  check('0 still rejected for cluster.rf_below_min', sent.length === 0 && msg() === '✗ Warning at must be a number greater than 0', msg());

  const row = rowCell(34);
  check('table: "info 1 · warn 0.5 · crit 0 msgs/sec", type label', row.querySelectorAll('td')[4].textContent === 'info 1 · warn 0.5 · crit 0 msgs/sec'
        && row.querySelectorAll('td')[1].textContent.startsWith('Message rate in'), row.querySelectorAll('td')[4].textContent);
  row.querySelector('button.config-edit').click();
  check('edit fills 1 / 0.5 / 0 (zero not treated as missing), below hint shown', $('af-tier-info').value === '1' && $('af-tier-warning').value === '0.5'
        && $('af-tier-critical').value === '0' && shown('af-tiers-below-hint') && !shown('af-tiers-above-hint'));
  sent = await save(); b = sent[0] && sent[0].body;
  check('edit PUT sends the same numbers, no min_rf', sent.length === 1 && sent[0].method === 'PUT' && JSON.stringify(b.tiers) === '{"info":1,"warning":0.5,"critical":0}' && !('min_rf' in b), JSON.stringify(b));
  check('msg_rate row has Copy (cluster rule)', !!rowCell(34).querySelector('button.config-copy'));

  console.log(fails ? `${fails} FAILED` : 'ALL PASSED');
  process.exit(fails ? 1 : 0);
})();
