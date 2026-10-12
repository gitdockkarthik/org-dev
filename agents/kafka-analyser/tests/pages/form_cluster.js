const { TEAMS_HTML } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');

const calls = [];
let clusters = [{ id: 4, name: 'c4', enabled: true }, { id: 8, name: 'c8', enabled: true }, { id: 9, name: 'c9', enabled: true }];
let clustersFail = false;
const configs = [
  { id: 3, name: 'Broker unreachable', alert_type: 'broker_unreachable', cluster_id: null, cluster_name: 'All clusters', severity: 'critical', threshold: 1, config: { threshold: 1 }, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, enabled: true, email_enabled: false, open_trigger_count: 0 },
  { id: 20, name: 'Broker CPU % - c4', alert_type: 'broker.cpu_pct', cluster_id: 4, cluster_name: 'c4', severity: 'warning', threshold: 1, config: { mode: 'simple', warning: 70, critical: 85 }, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, enabled: false, email_enabled: false, open_trigger_count: 0 },
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
      if (url.endsWith('/settings/clusters')) {
        if (clustersFail) return json({ detail: 'backend down' }, 503);
        return json({ clusters });
      }
      if (url.includes('/alerts/triggers')) return json({ triggers: [], total_count: 0 });
      if (url.endsWith('/alerts/configs') && method === 'GET') return json({ configs });
      calls.push({ method, url, body: opts.body ? JSON.parse(opts.body) : null });
      return json({ config: {} });
    };
  },
});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '  ' + extra : ''}`); };
const allOpt = () => $('af-cluster').querySelector('option[value=""]');
const state = () => `(all-clusters hidden=${allOpt().hidden} disabled=${allOpt().disabled}, selectedIndex=${$('af-cluster').selectedIndex}, value='${$('af-cluster').value}', hint='${$('af-cluster-hint').textContent}')`;
const setType = v => { $('af-alert-type').value = v; $('af-alert-type').dispatchEvent(new w.Event('change')); };
const setCluster = v => { $('af-cluster').value = v; $('af-cluster').dispatchEvent(new w.Event('change')); };
const newForm = (name, type) => { $('btn-new-alert').click(); $('af-name').value = name; if (type) setType(type); };
async function save() { const n = calls.length; $('btn-save-alert').click(); await flush(); return calls.slice(n); }
const msg = () => $('alert-form-msg').textContent;

(async () => {
  await flush();
  for (const type of ['broker.cpu_pct', 'broker.heap_pct']) {
    newForm('t ' + type, type);
    check(`${type}: "All clusters" unavailable, nothing selected`, allOpt().hidden && allOpt().disabled && $('af-cluster').selectedIndex === -1, state());
    $('af-tier-warning').value = '70';
    let sent = await save();
    check(`${type}: save without cluster rejected`, sent.length === 0 && msg() === '✗ Cluster is required for this metric', `msg='${msg()}', requests=${sent.length}`);
    setCluster('8');
    sent = await save();
    check(`${type}: save with cluster 8 POSTs cluster_id 8`, sent.length === 1 && sent[0].method === 'POST' && sent[0].body.cluster_id === 8, JSON.stringify(sent[0] && sent[0].body));
  }
  for (const type of ['close_wait_spike', 'broker_unreachable']) {
    newForm('g ' + type, type);
    check(`${type}: "All clusters" available and selected`, !allOpt().hidden && !allOpt().disabled && $('af-cluster').value === '' && $('af-cluster').selectedIndex === 0, state());
    const sent = await save();
    check(`${type}: save with no cluster POSTs cluster_id null`, sent.length === 1 && sent[0].body.cluster_id === null, JSON.stringify(sent[0] && sent[0].body));
  }
  // Switching type back and forth
  newForm('switch');
  setType('broker.cpu_pct');
  check('switch close_wait -> cpu: nothing selected', $('af-cluster').selectedIndex === -1 && allOpt().hidden, state());
  setType('close_wait_spike');
  check('switch cpu -> close_wait: "All clusters" back and selected', !allOpt().hidden && $('af-cluster').value === '' && $('af-cluster').selectedIndex === 0, state());
  setType('broker.heap_pct');
  check('switch close_wait -> heap: nothing selected again', $('af-cluster').selectedIndex === -1 && allOpt().disabled, state());
  setCluster('9');
  setType('broker_unreachable');
  check('heap on 9 -> broker_unreachable: keeps cluster 9, option available', $('af-cluster').value === '9' && !allOpt().hidden, state());
  setType('broker.cpu_pct');
  check('-> cpu: keeps cluster 9', $('af-cluster').value === '9' && allOpt().hidden, state());
  $('btn-cancel-alert').click();

  // Edit CPU rule on cluster 4, untouched
  d.querySelector('[data-id="20"].btn-edit, button[data-id="20"]') ? null : null;
  w.eval('openEditForm(_alertConfigs.find(c => c.id === 20))');
  check('edit cpu rule 20: cluster 4 shown', $('af-cluster').value === '4' && allOpt().hidden, state());
  $('af-name').value = 'Broker CPU % - c4 renamed';
  let sent = await save();
  check('edit untouched: PUT without cluster_id', sent.length === 1 && sent[0].method === 'PUT' && sent[0].url.endsWith('/alerts/configs/20') && !('cluster_id' in sent[0].body), JSON.stringify(sent[0] && sent[0].body));
  // Edit general rule 3 untouched (as before)
  w.eval('openEditForm(_alertConfigs.find(c => c.id === 3))');
  check('edit general rule 3: "All clusters" available and selected', !allOpt().hidden && $('af-cluster').value === '', state());
  sent = await save();
  check('edit general rule 3 untouched: PUT without cluster_id', sent.length === 1 && !('cluster_id' in sent[0].body), JSON.stringify(sent[0] && sent[0].body));

  // Rebuild cluster list while form is open
  newForm('rebuild', 'broker.cpu_pct');
  clusters = [...clusters, { id: 10, name: 'c10', enabled: true }];
  await w.eval('loadClusterOptions()');
  check('rebuild while cpu open, nothing selected: option still hidden, nothing selected', allOpt().hidden && $('af-cluster').selectedIndex === -1 && $('af-cluster').options.length === 5, state());
  $('af-tier-warning').value = '70';
  sent = await save();
  check('after rebuild: save without cluster still rejected', sent.length === 0 && msg() === '✗ Cluster is required for this metric', `msg='${msg()}'`);
  setCluster('8');
  await w.eval('loadClusterOptions()');
  check('rebuild with cluster 8 selected: keeps 8', $('af-cluster').value === '8' && allOpt().hidden, state());
  setType('close_wait_spike');
  await w.eval('loadClusterOptions()');
  check('rebuild while close_wait open: option available', !allOpt().hidden && !allOpt().disabled, state());
  clustersFail = true;
  setType('broker.cpu_pct');
  setCluster('');
  await w.eval('loadClusterOptions()');
  check('cluster load fails, cpu: hint explains, option still hidden', allOpt().hidden && /needs a cluster/.test($('af-cluster-hint').textContent), state());
  setType('close_wait_spike');
  check('cluster load fails, close_wait: original hint text', $('af-cluster-hint').textContent === 'Could not load clusters (backend down) — only "All clusters" is available.', state());
  console.log(fails ? `${fails} FAILED` : 'ALL PASS');
})();
