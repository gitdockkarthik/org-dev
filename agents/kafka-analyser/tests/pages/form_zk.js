const { TEAMS_HTML, groupMatchesAlertTypes, consecutive, tierHintProblems } = require('./_page');
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
  { ...base, id: 35, name: 'Data max - c8', alert_type: 'cluster.data_gb_max', cluster_id: 8, cluster_name: 'c8', config: { mode: 'simple', warning: 5000, critical: 8000 }, min_rf: null },
  { ...base, id: 37, name: 'ZK - c9', alert_type: 'cluster.zk_ensemble', cluster_id: 9, cluster_name: 'c9', config: { mode: 'simple', warning: 1, critical: 2 }, min_rf: null },
  { ...base, id: 36, name: 'Data spread - c8', alert_type: 'cluster.data_spread_pct', cluster_id: 8, cluster_name: 'c8', config: { mode: 'simple', warning: 10, critical: 25 }, min_rf: null },
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
  const T = 'cluster.zk_ensemble';
  const grp = $('af-alert-type').querySelector('optgroup[label="Cluster"]');
  const opts = [...grp.querySelectorAll('option')].map(o => o.value);
  check('type under Cluster, right after the data types, label matches API; Cluster group = ALERT_TYPES', consecutive(opts, ['cluster.data_gb_max', 'cluster.data_spread_pct', 'cluster.zk_ensemble']) && groupMatchesAlertTypes(w, 'Cluster')
        && grp.querySelector(`option[value="${T}"]`).textContent === 'ZooKeeper ensemble problems', JSON.stringify(opts));
  const other = [...$('af-alert-type').querySelectorAll('optgroup')].filter(g => g.label !== 'Cluster').flatMap(g => [...g.querySelectorAll('option')].map(o => o.value));
  check('not in any other group', !other.includes(T));
  const shown = id => !$(id).classList.contains('hidden');
  newForm('vis');
  // Owners are exact; every type also follows the ALERT_TYPES rules (_page.tierHintProblems).
  const bad = tierHintProblems(w, setType, { 'af-tiers-zk-hint': T, 'af-tiers-data-max-hint': 'cluster.data_gb_max', 'af-tiers-data-spread-hint': 'cluster.data_spread_pct',
                                             'af-tiers-skew-hint': 'cluster.leader_skew', 'af-tiers-below-hint': 'cluster.msg_rate_in' });
  check('zk hint only for zk; other tier hints unchanged (all ' + $('af-alert-type').options.length + ' types checked)', bad.length === 0, bad.join(', '));
  check('hint text', $('af-tiers-zk-hint').textContent === "Counts problems in the cluster's ZooKeeper ensemble: one per node that does not answer, plus one if there is not exactly one leader. Checked live every 5 minutes (one mntr per node) and confirmed over two checks. With three nodes, 1 = degraded and 2 = quorum at risk.");
  setType(T);
  check('unit "(problems)", min RF hidden, resolve-card field shown', d.querySelector('.af-tier-unit').textContent === ' (problems)' && !shown('af-min-rf-field')
        && !$('af-resolve-card').closest('.field').classList.contains('hidden'));
  check('"All clusters" not offered (cluster rule)', [...$('af-cluster').options].every(o => o.value !== '' || o.hidden || o.disabled));
  $('btn-cancel-alert').click();

  newForm('new zk', T); setCluster('4');
  $('af-tier-warning').value = '1'; $('af-tier-critical').value = '2'; $('af-min-rf').value = '3';
  let sent = await save(), b = sent[0] && sent[0].body;
  check('create sends tiers 1 / 2 as numbers, no min_rf', sent.length === 1 && sent[0].method === 'POST' && b.alert_type === T && b.cluster_id === 4
        && b.tiers.warning === 1 && b.tiers.critical === 2 && typeof b.tiers.warning === 'number' && !('info' in b.tiers) && !('min_rf' in b), JSON.stringify(b));
  newForm('dec zk', T); setCluster('4'); $('af-tier-warning').value = '1.5';
  sent = await save(); b = sent[0] && sent[0].body;
  check('decimal 1.5 accepted', sent.length === 1 && b.tiers.warning === 1.5, JSON.stringify(b));
  newForm('zero zk', T); setCluster('4'); $('af-tier-warning').value = '0';
  let z = await save();
  check('0 rejected', z.length === 0 && msg() === '✗ Warning at must be a number greater than 0', msg());
  newForm('nocluster zk', T); $('af-tier-warning').value = '1';
  z = await save();
  check('no cluster -> not sent', z.length === 0, msg());

  const cells = rowCell(37).querySelectorAll('td');
  check('table: "warn 1 · crit 2 problems", type label', cells[4].textContent === 'warn 1 · crit 2 problems' && cells[1].textContent.startsWith('ZooKeeper ensemble problems'), cells[4].textContent + ' | ' + cells[1].textContent);
  rowCell(37).querySelector('button.config-edit').click();
  check('edit 37 fills 1 / 2, type and cluster 9, zk hint shown', $('af-tier-warning').value === '1' && $('af-tier-critical').value === '2'
        && $('af-alert-type').value === T && $('af-cluster').value === '9' && shown('af-tiers-zk-hint'));
  $('af-tier-info').value = '1'; $('af-tier-warning').value = '2'; $('af-tier-critical').value = '3';
  sent = await save(); b = sent[0] && sent[0].body;
  check('edit 37 PUTs info 1 / warning 2 / critical 3, no min_rf', sent.length === 1 && sent[0].method === 'PUT' && sent[0].url.endsWith('/alerts/configs/37')
        && JSON.stringify(b.tiers) === '{"info":1,"warning":2,"critical":3}' && !('min_rf' in b), JSON.stringify(b));
  check('row 37 has Copy', !!rowCell(37).querySelector('button.config-copy'));

  console.log(fails ? `${fails} FAILED` : 'ALL PASSED');
  process.exit(fails ? 1 : 0);
})();
