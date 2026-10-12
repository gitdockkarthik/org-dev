const { TEAMS_HTML } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');

const calls = []; let configGets = 0;
let clusters = [{ id: 4, name: 'c4', enabled: true }, { id: 8, name: 'c8', enabled: true }, { id: 9, name: 'c9', enabled: true }];
let createReply = () => json({ config: { id: 100, cluster_id: 4, cluster_name: 'c4' } });
let copyReply = () => json({ preview: false, created: [{ id: 101, cluster_id: 8 }], skipped: [{ cluster_id: 9, reason: 'exists (id 50)' }] });
const configs = [{ id: 20, name: 'Broker heap % - c4', alert_type: 'broker.heap_pct', cluster_id: 4, cluster_name: 'c4', severity: 'warning', threshold: 1,
  config: { mode: 'simple', warning: 70 }, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, enabled: true, email_enabled: false, open_trigger_count: 0 }];
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });

const dom = new JSDOM(html, { runScripts: 'dangerously', url: 'http://localhost/', beforeParse(w) {
  w.Element.prototype.scrollIntoView = () => {};
  w.mountUserBadge = () => {}; w.fetchCurrentUser = async () => null;
  w.fetch = async (url, opts = {}) => {
    const method = opts.method || 'GET';
    if (url.endsWith('/settings/clusters')) return json({ clusters });
    if (url.includes('/alerts/triggers')) return json({ triggers: [], total_count: 0 });
    if (url.endsWith('/alerts/configs') && method === 'GET') { configGets++; return json({ configs }); }
    const body = opts.body ? JSON.parse(opts.body) : null;
    calls.push({ method, path: url.replace(/^.*?(\/alerts)/, '$1'), body });
    if (url.endsWith('/alerts/configs/copy')) return copyReply(body);
    if (url.endsWith('/alerts/configs')) return createReply(body);
    return json({ config: {} });
  };
}});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '\n    ' + extra : ''}`); };
const rowShown = () => !$('af-all-clusters-row').classList.contains('hidden');
const setType = v => { $('af-alert-type').value = v; $('af-alert-type').dispatchEvent(new w.Event('change')); };
const tickEvery = on => { $('af-all-clusters').checked = on; $('af-all-clusters').dispatchEvent(new w.Event('change')); };
const newCpu = () => { $('btn-new-alert').click(); $('af-name').value = 'CPU high'; setType('broker.cpu_pct'); $('af-tier-warning').value = '70'; $('af-tier-critical').value = '85'; };
async function save() { const n = calls.length, g = configGets; $('btn-save-alert').click(); await flush(); return { sent: calls.slice(n), reloads: configGets - g }; }
const fmt = sent => sent.map(c => `${c.method} ${c.path} ${JSON.stringify(c.body)}`).join('\n    ');

(async () => {
  await flush();
  $('btn-new-alert').click();
  for (const t of ['close_wait_spike', 'broker_unreachable']) { setType(t); check(`checkbox hidden for ${t}`, !rowShown()); }
  for (const t of ['broker.cpu_pct', 'broker.heap_pct']) { setType(t); check(`checkbox shown for ${t} on create`, rowShown() && !$('af-all-clusters').checked); }
  tickEvery(true);
  check('ticked: cluster select disabled, hint updated', $('af-cluster').disabled, `hint='${$('af-cluster-hint').textContent}'`);
  tickEvery(false);
  check('unticked: cluster select restored', !$('af-cluster').disabled, `hint='${$('af-cluster-hint').textContent}'`);
  tickEvery(true); setType('close_wait_spike');
  check('ticked then switch to general type: hidden, unticked, select enabled', !rowShown() && !$('af-all-clusters').checked && !$('af-cluster').disabled);
  $('btn-cancel-alert').click();

  w.eval('openEditForm(_alertConfigs.find(c => c.id === 20))');
  check('hidden on edit (heap rule)', !rowShown() && !$('af-cluster').disabled);
  $('btn-cancel-alert').click();

  // Success path
  newCpu(); tickEvery(true);
  let r = await save();
  check('ticked, no cluster: two requests in order, first enabled false on cluster 4, copy with config_id 100 and [8, 9]',
    r.sent.length === 2 && r.sent[0].path === '/alerts/configs' && r.sent[0].body.cluster_id === 4 && r.sent[0].body.enabled === false && r.sent[0].body.name === 'CPU high'
    && r.sent[1].path === '/alerts/configs/copy' && JSON.stringify(r.sent[1].body) === '{"config_id":100,"target_cluster_ids":[8,9],"preview":false}', fmt(r.sent));
  check('success: form closed and reset, list reloaded, message counts', $('alert-form-card').classList.contains('hidden') && !$('af-all-clusters').checked && r.reloads === 1 && $('configs-msg').textContent === '✓ Created 2 rule(s), 1 skipped', `configs-msg='${$('configs-msg').textContent}' reloads=${r.reloads}`);

  // Step 2 failure
  copyReply = () => json({ detail: 'Database not available' }, 500);
  newCpu(); tickEvery(true);
  r = await save();
  check('copy fails: form closed, list reloaded, partial-failure message kept', r.sent.length === 2 && $('alert-form-card').classList.contains('hidden') && r.reloads === 1
    && $('configs-msg').textContent === '✗ Rule created on c4, but copying to the other clusters failed: Database not available. Use the Copy button on that rule.' && $('configs-msg').classList.contains('err'), `configs-msg='${$('configs-msg').textContent}'`);
  await new Promise(res => setTimeout(res, 5200));
  check('partial-failure message not auto-hidden after 5 s', $('configs-msg').textContent.startsWith('✗ Rule created on c4'));

  // Step 1 failure
  createReply = () => json({ detail: 'A rule of this type already exists for this cluster (id 20)' }, 409);
  newCpu(); tickEvery(true);
  r = await save();
  check('create fails: error shown as today, no copy request, form stays open', r.sent.length === 1 && r.sent[0].path === '/alerts/configs' && !$('alert-form-card').classList.contains('hidden')
    && $('alert-form-msg').textContent === '✗ Save failed: A rule of this type already exists for this cluster (id 20)', `${fmt(r.sent)}\n    msg='${$('alert-form-msg').textContent}'`);
  $('btn-cancel-alert').click();
  createReply = () => json({ config: { id: 100, cluster_id: 4, cluster_name: 'c4' } });

  // Unticked create unchanged
  newCpu(); $('af-cluster').value = '8';
  r = await save();
  check('unticked create: one POST, exactly the old payload', r.sent.length === 1 && JSON.stringify(r.sent[0].body) === JSON.stringify({ name: 'CPU high', alert_type: 'broker.cpu_pct', cluster_id: 8, severity: 'warning', threshold: 1, cooldown_minutes: 30, webhook_url: null, enabled: true, send_resolve_card: true, tiers: { warning: 70, critical: 85 } })
    && $('configs-msg').textContent === '✓ Alert rule "CPU high" created', fmt(r.sent));

  // One cluster only
  clusters = [{ id: 4, name: 'c4', enabled: true }];
  await w.eval('loadClusterOptions()');
  newCpu(); tickEvery(true);
  r = await save();
  check('one cluster only: message, no request', r.sent.length === 0 && $('alert-form-msg').textContent === '✗ Only one cluster is available - use the cluster selector', `msg='${$('alert-form-msg').textContent}'`);
  console.log(fails ? `${fails} FAILED` : 'ALL PASS');
})();
