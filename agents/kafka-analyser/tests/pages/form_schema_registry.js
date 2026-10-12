const { TEAMS_HTML, alertTypes, groupMatchesAlertTypes, consecutive, tierHintProblems } = require('./_page');
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
  { ...base, id: 40, name: 'Connect FC - c4', alert_type: 'cluster.connect_failed_connectors', cluster_id: 4, cluster_name: 'c4', config: { mode: 'simple', warning: 1, critical: 5 }, min_rf: null },
  { ...base, id: 41, name: 'Connect FT - c8', alert_type: 'cluster.connect_failed_tasks', cluster_id: 8, cluster_name: 'c8', config: { mode: 'simple', info: 1, warning: 3, critical: 10 }, min_rf: null },
  { ...base, id: 42, name: 'Connect WD - c9', alert_type: 'cluster.connect_workers_down', cluster_id: 9, cluster_name: 'c9', config: { mode: 'simple', critical: 2 }, min_rf: null },
  { ...base, id: 43, name: 'SR nodes - c4', alert_type: 'cluster.sr_nodes_down', cluster_id: 4, cluster_name: 'c4', config: { mode: 'simple', warning: 1, critical: 2 }, min_rf: null },
  { ...base, id: 44, name: 'SR soft - c9', alert_type: 'cluster.sr_soft_deleted', cluster_id: 9, cluster_name: 'c9', config: { mode: 'simple', info: 5, critical: 50 }, min_rf: null },
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
  const shown = id => !$(id).classList.contains('hidden');
  const CONNECT = { 'af-tiers-connect-connectors-hint': 'cluster.connect_failed_connectors', 'af-tiers-connect-tasks-hint': 'cluster.connect_failed_tasks',
                    'af-tiers-connect-workers-hint': 'cluster.connect_workers_down' };
  const TYPES = [
    { v: 'cluster.sr_nodes_down', label: 'Schema Registry nodes down', unit: 'nodes', hint: 'af-tiers-sr-nodes-hint', id: 43, table: 'warn 1 · crit 2 nodes', cl: '4', fill: { warning: '1', critical: '2' },
      text: 'Counts Schema Registry nodes that do not answer a light request (GET /) within 3 seconds, checked live every 5 minutes and confirmed over two checks. Any HTTP answer below 500 counts as up.' },
    { v: 'cluster.sr_soft_deleted', label: 'Schema Registry soft-deleted subjects', unit: 'subjects', hint: 'af-tiers-sr-soft-deleted-hint', id: 44, table: 'info 5 · crit 50 subjects', cl: '9', fill: { info: '5', critical: '50' },
      text: 'Counts subjects that are soft-deleted but not purged (subjects listed with deleted=true minus live subjects). The card names up to 5. It does not count versions per subject. Clusters whose registry requires credentials (for example External Staging) are skipped.' },
  ];
  const grp = $('af-alert-type').querySelector('optgroup[label="Cluster"]');
  const opts = [...grp.querySelectorAll('option')].map(o => o.value);
  check('both types under Cluster, right after the three Connect types, in order; Cluster group = ALERT_TYPES', consecutive(opts, [...Object.values(CONNECT), ...TYPES.map(t => t.v)]) && groupMatchesAlertTypes(w, 'Cluster'), JSON.stringify(opts));
  check('labels match the API _TYPE_LABELS', TYPES.every(t => grp.querySelector(`option[value="${t.v}"]`).textContent === t.label));
  const other = [...$('af-alert-type').querySelectorAll('optgroup')].filter(g => g.label !== 'Cluster').flatMap(g => [...g.querySelectorAll('option')].map(o => o.value));
  check('not in any other group (not general)', TYPES.every(t => !other.includes(t.v)));

  // Every type: which hints are shown. Each Connect hint only for its own type; earlier hints unchanged.
  const HINTS = { 'af-tiers-zk-hint': 'cluster.zk_ensemble', 'af-tiers-data-max-hint': 'cluster.data_gb_max', 'af-tiers-data-spread-hint': 'cluster.data_spread_pct',
                  'af-tiers-skew-hint': 'cluster.leader_skew', 'af-tiers-below-hint': 'cluster.msg_rate_in', ...CONNECT, ...Object.fromEntries(TYPES.map(t => [t.hint, t.v])) };
  newForm('vis');
  // Owners above are exact; every type also follows the ALERT_TYPES rules (_page.tierHintProblems).
  const bad = tierHintProblems(w, setType, HINTS);
  check('each hint shown only for its own type (all ' + $('af-alert-type').options.length + ' types checked)', bad.length === 0 && $('af-alert-type').options.length === alertTypes(w).length, bad.join(', '));
  check('hint texts exact', TYPES.every(t => $(t.hint).textContent === t.text));
  for (const t of TYPES) {
    setType(t.v);
    check(`${t.label}: unit "(${t.unit})", tiers shown, severity/min RF hidden, resolve-card shown, no "All clusters"`,
          [...d.querySelectorAll('.af-tier-unit')].every(e => e.textContent === ` (${t.unit})`) && shown('af-tiers-field') && !shown('af-min-rf-field')
          && $('af-severity').closest('.field').classList.contains('hidden') && !$('af-resolve-card').closest('.field').classList.contains('hidden')
          && [...$('af-cluster').options].every(o => o.value !== '' || o.hidden || o.disabled));
  }
  $('btn-cancel-alert').click();

  for (const t of TYPES) {
    console.log(`-- ${t.label}`);
    newForm('new ' + t.v, t.v); setCluster('4');
    $('af-tier-warning').value = '1'; $('af-tier-critical').value = '5'; $('af-min-rf').value = '3';
    let sent = await save(), b = sent[0] && sent[0].body;
    check('create: POST, tiers {warning 1, critical 5} as numbers, no min_rf, resolve card sent', sent.length === 1 && sent[0].method === 'POST' && b.alert_type === t.v
          && b.cluster_id === 4 && JSON.stringify(b.tiers) === '{"warning":1,"critical":5}' && !('min_rf' in b) && b.send_resolve_card === true, JSON.stringify(b));
    check('  form closed, success message', $('alert-form-card').classList.contains('hidden') && $('configs-msg').textContent.includes('created'));
    newForm('zero', t.v); setCluster('4'); $('af-tier-warning').value = '0';
    let z = await save();
    check('0 rejected in the page, nothing sent', z.length === 0 && msg() === '✗ Warning at must be a number greater than 0', msg());
    newForm('dec', t.v); setCluster('4'); $('af-tier-warning').value = '1.5';
    nextResponse = json({ detail: 'tiers.warning must be a whole number greater than 0' }, 422);
    z = await save();
    check("decimal 1.5 sent as-is; the API's 422 message shown, form stays open", z.length === 1 && z[0].body.tiers.warning === 1.5
          && msg() === '✗ Save failed: tiers.warning must be a whole number greater than 0' && !$('alert-form-card').classList.contains('hidden'), msg());
    $('btn-cancel-alert').click();

    const cells = rowCell(t.id).querySelectorAll('td');
    check(`table: "${t.table}", type label`, cells[4].textContent === t.table && cells[1].textContent.startsWith(t.label), cells[4].textContent + ' | ' + cells[1].textContent);
    check('  row has Edit and Copy', !!rowCell(t.id).querySelector('button.config-edit') && !!rowCell(t.id).querySelector('button.config-copy'));
    rowCell(t.id).querySelector('button.config-edit').click();
    const filled = Object.entries(TIER_IDS_()).every(([tier, id]) => $(id).value === (t.fill[tier] || ''));
    check(`edit ${t.id} fills tiers, type, cluster ${t.cl}, own hint`, filled && $('af-alert-type').value === t.v && $('af-cluster').value === t.cl && shown(t.hint)
          && TYPES.every(o => o === t || !shown(o.hint)));
    $('af-tier-info').value = '2'; $('af-tier-warning').value = '4'; $('af-tier-critical').value = '8';
    sent = await save(); b = sent[0] && sent[0].body;
    check(`edit ${t.id} PUTs info 2 / warning 4 / critical 8, no min_rf`, sent.length === 1 && sent[0].method === 'PUT' && sent[0].url.endsWith('/alerts/configs/' + t.id)
          && JSON.stringify(b.tiers) === '{"info":2,"warning":4,"critical":8}' && !('min_rf' in b), JSON.stringify(b));
  }

  console.log(fails ? `${fails} FAILED` : 'ALL PASSED');
  process.exit(fails ? 1 : 0);
})();
function TIER_IDS_() { return { info: 'af-tier-info', warning: 'af-tier-warning', critical: 'af-tier-critical' }; }
