const { TEAMS_HTML, groupMatchesAlertTypes } = require('./_page');
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
  // Dropdown
  const grp = $('af-alert-type').querySelector('optgroup[label="Cluster"]');
  const opts = grp ? [...grp.querySelectorAll('option')].map(o => `${o.value}=${o.textContent}`) : [];
  check('both types first under optgroup "Cluster", with labels; Cluster group = ALERT_TYPES', JSON.stringify(opts.slice(0, 2)) === JSON.stringify(['cluster.urp_total=Under-replicated partitions', 'cluster.rf_below_min=Topics below minimum RF'])
        && groupMatchesAlertTypes(w, 'Cluster'), JSON.stringify(opts));

  // Field visibility
  newForm('vis');
  const vis = {};
  for (const t of ['close_wait_spike', 'broker_unreachable', 'broker.cpu_pct', 'broker.heap_pct', 'cluster.urp_total', 'cluster.rf_below_min']) {
    setType(t); vis[t] = minRfShown();
  }
  check('min RF field shown only for cluster.rf_below_min', Object.entries(vis).every(([t, s]) => s === (t === 'cluster.rf_below_min')), JSON.stringify(vis));
  setType('cluster.rf_below_min');
  check('min RF input: number, min 2, step 1; label and hint', $('af-min-rf').type === 'number' && $('af-min-rf').min === '2' && $('af-min-rf').step === '1'
        && d.querySelector('label[for="af-min-rf"]').textContent === 'Minimum replication factor'
        && $('af-min-rf-field').querySelector('.hint').textContent.includes('min.insync.replicas is not checked'));
  check('cluster rule: "All clusters" not offered, every-cluster box offered', $('af-cluster').querySelector('option[value=""]').hidden && !$('af-all-clusters-row').classList.contains('hidden'));
  $('btn-cancel-alert').click();

  // Validation
  for (const bad of ['', '1', '2.5', 'abc', '0', '-3']) {
    newForm('rf bad', 'cluster.rf_below_min'); setCluster('9');
    $('af-tier-warning').value = '5';
    $('af-min-rf').value = bad;
    const sent = await save();
    check(`min_rf ${JSON.stringify(bad)} (input reads ${JSON.stringify($('af-min-rf').value)}) rejected inline`, sent.length === 0 && msg() === '✗ Minimum replication factor must be a whole number of at least 2'
          && $('alert-form-msg').className === 'action-msg err', `msg='${msg()}'`);
  }

  // Create RF
  newForm('RF - c9', 'cluster.rf_below_min'); setCluster('9');
  $('af-tier-warning').value = '5'; $('af-tier-critical').value = '20'; $('af-min-rf').value = '3';
  let sent = await save();
  let b = sent[0] && sent[0].body;
  check('RF create: one POST, min_rf 3 as integer, tiers, cluster 9', sent.length === 1 && sent[0].method === 'POST' && b.min_rf === 3 && Number.isInteger(b.min_rf)
        && JSON.stringify(b.tiers) === '{"warning":5,"critical":20}' && b.cluster_id === 9 && b.alert_type === 'cluster.rf_below_min', JSON.stringify(b));
  check('form reset clears af-min-rf', $('af-min-rf').value === '');

  // Create URP
  newForm('URP - c9', 'cluster.urp_total'); setCluster('9');
  $('af-tier-critical').value = '100';
  $('af-min-rf').value = '4';  // stale value in a hidden field must not be sent
  sent = await save(); b = sent[0] && sent[0].body;
  check('URP create: no min_rf, tiers sent', sent.length === 1 && !('min_rf' in b) && JSON.stringify(b.tiers) === '{"critical":100}', JSON.stringify(b));
  newForm('CPU', 'broker.cpu_pct'); setCluster('9'); $('af-tier-warning').value = '70';
  sent = await save(); b = sent[0] && sent[0].body;
  check('CPU create: no min_rf', sent.length === 1 && !('min_rf' in b), JSON.stringify(b));

  // Edit
  rowCell(30).querySelector('button.config-edit').click();
  check('edit RF rule 30: min RF field shown and filled with 3', minRfShown() && $('af-min-rf').value === '3' && $('af-tier-warning').value === '5');
  sent = await save(); b = sent[0] && sent[0].body;
  check('untouched edit PUTs min_rf 3 and tiers, no alert_type/cluster', sent.length === 1 && sent[0].method === 'PUT' && sent[0].url.endsWith('/alerts/configs/30')
        && b.min_rf === 3 && JSON.stringify(b.tiers) === '{"warning":5,"critical":20}' && !('alert_type' in b) && !('cluster_id' in b), JSON.stringify(b));
  rowCell(30).querySelector('button.config-edit').click();
  $('af-min-rf').value = '1';
  sent = await save();
  check('edit with min_rf 1 rejected inline', sent.length === 0 && msg() === '✗ Minimum replication factor must be a whole number of at least 2');
  $('af-min-rf').value = '4';
  sent = await save();
  check('edit with min_rf 4 PUTs 4', sent.length === 1 && sent[0].body.min_rf === 4);
  rowCell(31).querySelector('button.config-edit').click();
  check('edit URP rule: min RF field hidden and empty', !minRfShown() && $('af-min-rf').value === '');
  sent = await save();
  check('URP edit PUT has no min_rf', sent.length === 1 && !('min_rf' in sent[0].body), JSON.stringify(sent[0] && sent[0].body));
  $('btn-cancel-alert').click();

  // Table
  const cellText = id => rowCell(id).querySelectorAll('td')[4].innerHTML;
  check('RF row: "RF < 3 · warn 5 · crit 20 topics"', cellText(30) === 'RF &lt; 3 · warn 5 · crit 20 topics', cellText(30));
  check('URP row unchanged format', cellText(31) === 'info 1 · crit 100 partitions', cellText(31));
  check('CPU row unchanged', cellText(20) === 'warn 70 · crit 85 %', cellText(20));
  check('min_rf escaped in table', cellText(32) === 'RF &lt; &lt;b&gt;2&lt;/b&gt; · warn 1 topics' && !rowCell(32).querySelector('td b'), cellText(32));
  check('type labels in table', rowCell(30).querySelectorAll('td')[1].textContent.startsWith('Topics below minimum RF') && rowCell(31).querySelectorAll('td')[1].textContent.startsWith('Under-replicated partitions'));

  // 422 shapes
  newForm('RF - c9', 'cluster.rf_below_min'); setCluster('9'); $('af-tier-warning').value = '5'; $('af-min-rf').value = '3';
  nextResponse = json({ detail: 'min_rf must be a whole number of at least 2' }, 422);
  await save();
  check('422 string detail shown', msg() === '✗ Save failed: min_rf must be a whole number of at least 2', msg());
  nextResponse = json({ detail: [{ type: 'value_error', loc: ['body', 'min_rf'], msg: 'Value error, min_rf must be a whole number of at least 2' }] }, 422);
  await save();
  check('422 pydantic list shown without "Value error, " or repeated field', msg() === '✗ Save failed: min_rf must be a whole number of at least 2', msg());
  nextResponse = json({ detail: [{ loc: ['body', 'cooldown_minutes'], msg: 'Input should be greater than or equal to 0' }, { loc: ['body', 'name'], msg: 'String should have at least 1 character' }] }, 422);
  await save();
  check('other pydantic errors keep "field: msg" joined by "; "', msg() === '✗ Save failed: cooldown_minutes: Input should be greater than or equal to 0; name: String should have at least 1 character', msg());
  nextResponse = json({ detail: 'A rule of this type already exists for this cluster (id 7)' }, 409);
  await save();
  check('409 string unchanged', msg() === '✗ Save failed: A rule of this type already exists for this cluster (id 7)', msg());
  nextResponse = { ok: false, status: 502, json: async () => { throw new Error('not json'); } };
  await save();
  check('non-JSON error unchanged', msg() === '✗ Save failed: HTTP 502', msg());
  $('btn-cancel-alert').click();

  // Create for every cluster
  newForm('Topics below minimum RF', 'cluster.rf_below_min');
  $('af-all-clusters').checked = true; $('af-all-clusters').dispatchEvent(new w.Event('change'));
  $('af-tier-warning').value = '5'; $('af-min-rf').value = '3';
  sent = await save();
  check('every cluster (4 and 8 have the type): single POST on 9 with min_rf 3, no copy',
        sent.length === 1 && sent[0].body.cluster_id === 9 && sent[0].body.min_rf === 3, JSON.stringify(sent.map(s => s.body)));
  configs.splice(configs.findIndex(c => c.id === 32), 1);
  await w.loadConfigs();
  newForm('Topics below minimum RF', 'cluster.rf_below_min');
  $('af-all-clusters').checked = true; $('af-all-clusters').dispatchEvent(new w.Event('change'));
  $('af-tier-warning').value = '5'; $('af-min-rf').value = '3';
  sent = await save();
  check('every cluster: first POST on cluster 8 (4 has the type) carries min_rf 3, then copy to [9]',
        sent.length === 2 && sent[0].method === 'POST' && sent[0].url.endsWith('/alerts/configs') && sent[0].body.cluster_id === 8 && sent[0].body.min_rf === 3 && sent[0].body.enabled === false
        && sent[1].url.endsWith('/alerts/configs/copy') && JSON.stringify(sent[1].body.target_cluster_ids) === '[9]' && sent[1].body.config_id === 100,
        JSON.stringify(sent.map(s => s.body)));
  newForm('every rf bad', 'cluster.rf_below_min');
  $('af-all-clusters').checked = true; $('af-all-clusters').dispatchEvent(new w.Event('change'));
  $('af-tier-warning').value = '5';
  sent = await save();
  check('every cluster without min_rf: nothing sent', sent.length === 0 && msg().includes('Minimum replication factor'));

  console.log(fails ? `${fails} FAILED` : 'ALL PASSED');
  process.exit(fails ? 1 : 0);
})();
