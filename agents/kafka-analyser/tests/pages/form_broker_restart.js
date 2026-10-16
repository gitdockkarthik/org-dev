// teams.html: the cluster.broker_restarted type (Broker restarts) in the rules form: group and label,
// its tier hint, unit, create / edit payloads, table cell, and "Create for every cluster".
const { TEAMS_HTML, groupMatchesAlertTypes, consecutive, tierHintProblems } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');

const calls = [];
const clusters = [{ id: 4, name: 'c4', enabled: true }, { id: 8, name: 'c8', enabled: true }, { id: 9, name: 'c9', enabled: true }];
const base = { severity: 'warning', threshold: 1, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, enabled: true, email_enabled: false, open_trigger_count: 0 };
const configs = [
  { ...base, id: 38, name: 'Restarts - c9', alert_type: 'cluster.broker_restarted', cluster_id: 9, cluster_name: 'c9', config: { mode: 'simple', info: 1, warning: 3, critical: 5 }, min_rf: null },
];
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });
const dom = new JSDOM(html, { runScripts: 'dangerously', url: 'http://localhost/', beforeParse(w) {
  w.Element.prototype.scrollIntoView = () => {};
  w.mountUserBadge = () => {}; w.fetchCurrentUser = async () => null;
  w.fetch = async (url, opts = {}) => {
    const method = opts.method || 'GET';
    if (url.endsWith('/settings/clusters')) return json({ clusters });
    if (url.includes('/alerts/triggers')) return json({ triggers: [], total_count: 0 });
    if (url.endsWith('/alerts/configs') && method === 'GET') return json({ configs });
    calls.push({ method, path: url.replace(/^.*?(\/alerts)/, '$1'), body: opts.body ? JSON.parse(opts.body) : null });
    if (url.endsWith('/alerts/configs/copy')) return json({ preview: false, created: [{ id: 101 }, { id: 102 }], skipped: [] });
    return json({ config: { id: 100, cluster_id: 4, cluster_name: 'c4' } });
  };
}});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '  ' + extra : ''}`); };
const setType = v => { $('af-alert-type').value = v; $('af-alert-type').dispatchEvent(new w.Event('change')); };
const setCluster = v => { $('af-cluster').value = v; $('af-cluster').dispatchEvent(new w.Event('change')); };
const newForm = (name, type) => { $('btn-new-alert').click(); $('af-name').value = name; if (type) setType(type); };
async function save() { const n = calls.length; $('btn-save-alert').click(); await flush(); return calls.slice(n); }
const msg = () => $('alert-form-msg').textContent;
const rowCell = id => [...d.querySelectorAll('#configs-body tr')].find(tr => tr.querySelector(`button.config-edit[data-id="${id}"]`));
const shown = id => !$(id).classList.contains('hidden');

(async () => {
  await flush();
  const T = 'cluster.broker_restarted';
  const grp = $('af-alert-type').querySelector('optgroup[label="Cluster"]');
  const opts = [...grp.querySelectorAll('option')].map(o => o.value);
  check('type under Cluster, right after leader skew, label "Broker restarts"; Cluster group = ALERT_TYPES',
    consecutive(opts, ['cluster.leader_skew', T]) && groupMatchesAlertTypes(w, 'Cluster') && grp.querySelector(`option[value="${T}"]`).textContent === 'Broker restarts', JSON.stringify(opts));
  const other = [...$('af-alert-type').querySelectorAll('optgroup')].filter(g => g.label !== 'Cluster').flatMap(g => [...g.querySelectorAll('option')].map(o => o.value));
  check('not in any other group', !other.includes(T));
  newForm('vis');
  const bad = tierHintProblems(w, setType, { 'af-tiers-broker-restart-hint': T, 'af-tiers-zk-hint': 'cluster.zk_ensemble', 'af-tiers-skew-hint': 'cluster.leader_skew' });
  check('restart hint only for this type; other tier hints unchanged (all ' + $('af-alert-type').options.length + ' types checked)', bad.length === 0, bad.join(', '));
  check('hint text names the window setting, the suggested tiers and resolve', /last N minutes \(Settings, Alert rules: 30 by default\)/.test($('af-tiers-broker-restart-hint').textContent)
    && /Suggested tiers: info 1, warning 3, critical 5/.test($('af-tiers-broker-restart-hint').textContent) && /Resolves when the restarts age out/.test($('af-tiers-broker-restart-hint').textContent));
  setType(T);
  check('above direction (above hint shown, below hints hidden), unit "(restarts)", min RF hidden, resolve-card field shown',
    shown('af-tiers-above-hint') && !shown('af-tiers-below-hint') && !shown('af-tiers-slo-hint') && d.querySelector('.af-tier-unit').textContent === ' (restarts)' && !shown('af-min-rf-field')
    && !$('af-resolve-card').closest('.field').classList.contains('hidden'));
  check('"All clusters" option not offered (cluster rule)', [...$('af-cluster').options].every(o => o.value !== '' || o.hidden || o.disabled));
  check('"Create for every cluster" offered on create', !$('af-all-clusters-row').classList.contains('hidden'));
  $('btn-cancel-alert').click();

  newForm('new restarts', T); setCluster('4');
  $('af-tier-info').value = '1'; $('af-tier-warning').value = '3'; $('af-tier-critical').value = '5';
  let sent = await save(), b = sent[0] && sent[0].body;
  check('create sends tiers 1 / 3 / 5 as numbers, no min_rf', sent.length === 1 && sent[0].method === 'POST' && b.alert_type === T && b.cluster_id === 4
    && b.tiers.info === 1 && b.tiers.warning === 3 && b.tiers.critical === 5 && typeof b.tiers.info === 'number' && !('min_rf' in b), JSON.stringify(b));
  newForm('zero', T); setCluster('4'); $('af-tier-warning').value = '0';
  let z = await save();
  check('0 rejected', z.length === 0 && msg() === '✗ Warning at must be a number greater than 0', msg());
  newForm('nocluster', T); $('af-tier-warning').value = '1';
  z = await save();
  check('no cluster -> not sent', z.length === 0, msg());

  newForm('every', T); $('af-all-clusters').checked = true; $('af-all-clusters').dispatchEvent(new w.Event('change'));
  $('af-tier-info').value = '1'; $('af-tier-warning').value = '3'; $('af-tier-critical').value = '5';
  sent = await save();
  check('every cluster: create on the first cluster (disabled), then copy to the clusters without one (cluster 9 already has a rule)',
    sent.length === 2 && sent[0].path === '/alerts/configs' && sent[0].body.cluster_id === 4 && sent[0].body.alert_type === T && sent[0].body.enabled === false
    && sent[1].path === '/alerts/configs/copy' && JSON.stringify(sent[1].body) === '{"config_id":100,"target_cluster_ids":[8],"preview":false}', JSON.stringify(sent));

  const cells = rowCell(38).querySelectorAll('td');
  check('table: "info 1 · warn 3 · crit 5 restarts", type label', cells[4].textContent === 'info 1 · warn 3 · crit 5 restarts' && cells[1].textContent.startsWith('Broker restarts'), cells[4].textContent + ' | ' + cells[1].textContent);
  rowCell(38).querySelector('button.config-edit').click();
  check('edit 38 fills 1 / 3 / 5, type and cluster 9, hint shown', $('af-tier-info').value === '1' && $('af-tier-warning').value === '3' && $('af-tier-critical').value === '5'
    && $('af-alert-type').value === T && $('af-cluster').value === '9' && shown('af-tiers-broker-restart-hint'));
  $('af-tier-critical').value = '8';
  sent = await save(); b = sent[0] && sent[0].body;
  check('edit 38 PUTs info 1 / warning 3 / critical 8', sent.length === 1 && sent[0].method === 'PUT' && sent[0].url === undefined && sent[0].path === '/alerts/configs/38'
    && JSON.stringify(b.tiers) === '{"info":1,"warning":3,"critical":8}', JSON.stringify(sent));
  check('row 38 has Copy', !!rowCell(38).querySelector('button.config-copy'));
  console.log(fails ? `${fails} FAILED` : 'ALL PASSED');
  process.exit(fails ? 1 : 0);
})();
