const { REPO, TEAMS_HTML } = require('./_page');
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
  const before = require('child_process').execSync(`git -C ${REPO} show HEAD:portal/agents/kafka-analyser/teams.html`).toString();
  const ids = [...new Set([...before.matchAll(/id="([^"]+)"/g)].map(m => m[1]).filter(id => !id.includes("${")))];
  const missing = ids.filter(id => !$(id));
  check(`all ${ids.length} ids from HEAD still exist`, missing.length === 0, JSON.stringify(missing));
  const hints = ['af-tiers-above-hint', 'af-tiers-below-hint', 'af-tiers-data-max-hint', 'af-tiers-data-spread-hint', 'af-tiers-zk-hint', 'af-tiers-skew-hint'];
  check('hints still inside af-tiers-field', hints.every(h => $(h).closest('#af-tiers-field')));

  const field = $('af-tiers-field'), grid = field.querySelector('.af-tiers-grid'), cs = el => w.getComputedStyle(el);
  check('thresholds field spans the full field-grid row', cs(field).gridColumn === '1 / -1' || cs(field).gridColumnStart === '1', cs(field).gridColumn);
  check('tier grid: display grid, align-items end, columns minmax(11rem, 14rem) auto-fit', cs(grid).display === 'grid' && cs(grid).alignItems === 'end'
        && /auto-fit.*minmax\(11rem, 14rem\)/.test(cs(grid).gridTemplateColumns), cs(grid).gridTemplateColumns);
  check('field grid columns wrap and cannot overflow', /auto-fill.*min\(220px, 100%\)/.test(cs(field.closest('.field-grid')).gridTemplateColumns), cs(field.closest('.field-grid')).gridTemplateColumns);
  const tiers = ['info', 'warning', 'critical'];
  check('each tier: label (nowrap, no unit inside) then [input + unit span]', tiers.every(t => {
    const inp = $('af-tier-' + t), col = inp.closest('.af-tier-input').parentElement, lab = col.querySelector(`label[for="af-tier-${t}"]`);
    return lab && cs(lab).whiteSpace === 'nowrap' && cs(lab).display === 'block' && !lab.querySelector('.af-tier-unit')
      && inp.nextElementSibling && inp.nextElementSibling.classList.contains('af-tier-unit') && cs(inp.parentElement).display === 'flex';
  }));
  check('inputs: equal flex 1 1 6rem, min-width 6rem, step any, min 0', tiers.every(t => {
    const c = cs($('af-tier-' + t)); return c.minWidth === '6rem' && c.flex.startsWith('1 1 6rem') && $('af-tier-' + t).step === 'any' && $('af-tier-' + t).min === '0';
  }), cs($('af-tier-info')).flex + ' / ' + cs($('af-tier-info')).minWidth);
  check('unit span nowrap', cs(d.querySelector('.af-tier-unit')).whiteSpace === 'nowrap');
  check('labels read "Info at", "Warning at", "Critical at"', tiers.every(t => d.querySelector(`label[for="af-tier-${t}"]`).textContent === t[0].toUpperCase() + t.slice(1) + ' at'));

  newForm('units');
  const units = { 'broker.cpu_pct': '%', 'cluster.data_gb_max': 'GB', 'cluster.msg_rate_in': 'msgs/sec', 'cluster.leader_skew': 'x average',
                  'cluster.urp_total': 'partitions', 'cluster.rf_below_min': 'topics', 'cluster.zk_ensemble': 'problems' };
  const got = {};
  for (const [t, u] of Object.entries(units)) { setType(t); got[u] = [...d.querySelectorAll('.af-tier-unit')].map(e => e.textContent); }
  check('unit suffix updates on all three inputs for the seven units', Object.entries(got).every(([u, arr]) => arr.length === 3 && arr.every(x => x === ` (${u})`)), JSON.stringify(got));
  setType('close_wait_spike');
  check('non-tier type clears the suffix', [...d.querySelectorAll('.af-tier-unit')].every(e => e.textContent === ''));
  $('btn-cancel-alert').click();

  newForm('cpu', 'broker.cpu_pct'); setCluster('4');
  $('af-tier-info').value = '60'; $('af-tier-warning').value = '70.5'; $('af-tier-critical').value = '90';
  let sent = await save(), b = sent[0] && sent[0].body;
  check('create reads and sends tiers as before', sent.length === 1 && JSON.stringify(b.tiers) === '{"info":60,"warning":70.5,"critical":90}', JSON.stringify(b && b.tiers));
  newForm('rate', 'cluster.msg_rate_in'); setCluster('4'); $('af-tier-warning').value = '0';
  sent = await save(); b = sent[0] && sent[0].body;
  check('below type still sends 0', sent.length === 1 && b.tiers.warning === 0, JSON.stringify(b && b.tiers));
  newForm('zero', 'cluster.zk_ensemble'); setCluster('4'); $('af-tier-warning').value = '0';
  sent = await save();
  check('0 still rejected for above types', sent.length === 0 && msg() === '✗ Warning at must be a number greater than 0', msg());
  rowCell(37).querySelector('button.config-edit').click();
  check('edit fills tiers and suffix', $('af-tier-warning').value === '1' && $('af-tier-critical').value === '2' && d.querySelector('.af-tier-unit').textContent === ' (problems)');
  $('af-tier-critical').value = '3';
  sent = await save(); b = sent[0] && sent[0].body;
  check('edit PUTs tiers as before', sent.length === 1 && sent[0].method === 'PUT' && JSON.stringify(b.tiers) === '{"warning":1,"critical":3}', JSON.stringify(b && b.tiers));

  console.log(fails ? `${fails} FAILED` : 'ALL PASSED');
  process.exit(fails ? 1 : 0);
})();
