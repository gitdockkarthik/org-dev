const { TEAMS_HTML } = require('./_page');
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
  { ...base, id: 33, name: 'Skew - c8', alert_type: 'cluster.leader_skew', cluster_id: 8, cluster_name: 'c8', config: { mode: 'simple', warning: 1.5, critical: 2.25 }, min_rf: null },
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
  check('leader skew under "Cluster" with its label', opts.includes('cluster.leader_skew=Leader partition skew'), JSON.stringify(opts));
  const hintShown = () => !$('af-tiers-skew-hint').classList.contains('hidden');
  newForm('vis');
  const vis = {};
  for (const o of $('af-alert-type').options) { setType(o.value); vis[o.value] = hintShown(); }
  check('skew hint shown only for cluster.leader_skew', Object.entries(vis).every(([t, s]) => s === (t === 'cluster.leader_skew')), JSON.stringify(vis));
  setType('cluster.leader_skew');
  check('hint text', $('af-tiers-skew-hint').textContent === "Ratio of the busiest broker's leader partitions to the cluster average (1.0 = balanced). Does not detect a broker that has lost all its leaders.");
  check('tiers shown, min RF hidden, unit "(x average)", tier inputs step any',
        !$('af-tiers-field').classList.contains('hidden') && $('af-min-rf-field').classList.contains('hidden')
        && d.querySelector('.af-tier-unit').textContent === ' (x average)' && $('af-tier-warning').step === 'any');
  $('btn-cancel-alert').click();

  newForm('Skew - c9', 'cluster.leader_skew'); setCluster('9');
  $('af-tier-warning').value = '1.5'; $('af-tier-critical').value = '2.25'; $('af-min-rf').value = '3';
  let sent = await save(), b = sent[0] && sent[0].body;
  check('create sends tiers 1.5 / 2.25 as numbers, no min_rf', sent.length === 1 && b.alert_type === 'cluster.leader_skew' && b.tiers.warning === 1.5 && b.tiers.critical === 2.25
        && typeof b.tiers.warning === 'number' && !('min_rf' in b) && b.cluster_id === 9, JSON.stringify(b));

  const row = rowCell(33);
  check('table: "warn 1.5 · crit 2.25 x average", type label', row.querySelectorAll('td')[4].textContent === 'warn 1.5 · crit 2.25 x average'
        && row.querySelectorAll('td')[1].textContent.startsWith('Leader partition skew'), row.querySelectorAll('td')[4].textContent);
  row.querySelector('button.config-edit').click();
  check('edit fills 1.5 / 2.25, hint shown', $('af-tier-warning').value === '1.5' && $('af-tier-critical').value === '2.25' && hintShown());
  sent = await save(); b = sent[0] && sent[0].body;
  check('edit PUT keeps decimals, no min_rf', sent.length === 1 && sent[0].method === 'PUT' && JSON.stringify(b.tiers) === '{"warning":1.5,"critical":2.25}' && !('min_rf' in b), JSON.stringify(b));
  rowCell(33).querySelector('button.config-copy') ? check('skew row has Copy (cluster rule)', true) : check('skew row has Copy (cluster rule)', false);
  newForm('Skew bad', 'cluster.leader_skew'); setCluster('9'); $('af-tier-warning').value = '0';
  sent = await save();
  check('tier 0 still rejected inline', sent.length === 0 && msg() === '✗ Warning at must be a number greater than 0', msg());

  console.log(fails ? `${fails} FAILED` : 'ALL PASSED');
  process.exit(fails ? 1 : 0);
})();
