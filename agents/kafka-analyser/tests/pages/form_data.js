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
  const grp = $('af-alert-type').querySelector('optgroup[label="Cluster"]');
  const opts = [...grp.querySelectorAll('option')].map(o => o.value);
  check('both types under Cluster, right after message rate; Cluster group = ALERT_TYPES', consecutive(opts, ['cluster.msg_rate_in', 'cluster.data_gb_max', 'cluster.data_spread_pct']) && groupMatchesAlertTypes(w, 'Cluster')
        && grp.querySelector('option[value="cluster.data_gb_max"]').textContent === 'Data on disk (max broker)'
        && grp.querySelector('option[value="cluster.data_spread_pct"]').textContent === 'Data spread between brokers', JSON.stringify(opts));
  const shown = id => !$(id).classList.contains('hidden');
  newForm('vis');
  // Owners are exact; every type also follows the ALERT_TYPES rules (_page.tierHintProblems).
  const bad = tierHintProblems(w, setType, { 'af-tiers-data-max-hint': 'cluster.data_gb_max', 'af-tiers-data-spread-hint': 'cluster.data_spread_pct',
                                             'af-tiers-skew-hint': 'cluster.leader_skew', 'af-tiers-below-hint': 'cluster.msg_rate_in' });
  check('each hint only for its own type; other tier hints unchanged (all ' + $('af-alert-type').options.length + ' types checked)', bad.length === 0, bad.join(', '));
  check('hint texts', $('af-tiers-data-max-hint').textContent === "Fires when the largest broker's data on disk (GB) reaches a tier. The card names that broker. The rule is ignored while any broker of the cluster is not reporting."
        && $('af-tiers-data-spread-hint').textContent === 'Spread = (largest - smallest broker data) / largest x 100. Fires when a broker holds much less data than the others (rebuilt, lost disks or not replicating). Normal spread is under 1%. Ignored while any broker is not reporting.');
  setType('cluster.data_gb_max');
  check('data max: unit "(GB)", min RF hidden', d.querySelector('.af-tier-unit').textContent === ' (GB)' && !shown('af-min-rf-field'));
  setType('cluster.data_spread_pct');
  check('data spread: unit "(%)"', d.querySelector('.af-tier-unit').textContent === ' (%)');
  $('btn-cancel-alert').click();

  for (const [t, w, c] of [['cluster.data_gb_max', '5000.5', '8000.25'], ['cluster.data_spread_pct', '10.5', '25']]) {
    newForm('new ' + t, t); setCluster('9');
    $('af-tier-warning').value = w; $('af-tier-critical').value = c; $('af-min-rf').value = '3';
    const sent = await save(), b = sent[0] && sent[0].body;
    check(`${t}: create sends ${w} / ${c} as numbers, no min_rf`, sent.length === 1 && b.alert_type === t && b.tiers.warning === parseFloat(w) && b.tiers.critical === parseFloat(c)
          && typeof b.tiers.warning === 'number' && !('min_rf' in b), JSON.stringify(b));
    newForm('zero ' + t, t); setCluster('9'); $('af-tier-warning').value = '0';
    const z = await save();
    check(`${t}: 0 rejected`, z.length === 0 && msg() === '✗ Warning at must be a number greater than 0', msg());
  }

  check('table: data max "warn 5000 · crit 8000 GB"', rowCell(35).querySelectorAll('td')[4].textContent === 'warn 5000 · crit 8000 GB' && rowCell(35).querySelectorAll('td')[1].textContent.startsWith('Data on disk (max broker)'), rowCell(35).querySelectorAll('td')[4].textContent);
  check('table: data spread "warn 10 · crit 25 %"', rowCell(36).querySelectorAll('td')[4].textContent === 'warn 10 · crit 25 %' && rowCell(36).querySelectorAll('td')[1].textContent.startsWith('Data spread between brokers'), rowCell(36).querySelectorAll('td')[4].textContent);
  for (const [id, w, c, hint] of [[35, '5000', '8000', 'af-tiers-data-max-hint'], [36, '10', '25', 'af-tiers-data-spread-hint']]) {
    rowCell(id).querySelector('button.config-edit').click();
    check(`edit ${id} fills ${w} / ${c}, its hint shown`, $('af-tier-warning').value === w && $('af-tier-critical').value === c && shown(hint));
    $('af-tier-warning').value = id === 35 ? '5500.5' : '12.5';
    const sent = await save(), b = sent[0] && sent[0].body;
    check(`edit ${id} PUTs the new decimal and keeps critical, no min_rf`, sent.length === 1 && sent[0].method === 'PUT' && b.tiers.warning === (id === 35 ? 5500.5 : 12.5) && b.tiers.critical === Number(c) && !('min_rf' in b), JSON.stringify(b));
    check(`row ${id} has Copy`, !!rowCell(id).querySelector('button.config-copy'));
  }

  console.log(fails ? `${fails} FAILED` : 'ALL PASSED');
  process.exit(fails ? 1 : 0);
})();
