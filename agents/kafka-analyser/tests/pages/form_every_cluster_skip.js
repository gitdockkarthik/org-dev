const { TEAMS_HTML } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');

const calls = [];
const clusters = [{ id: 4, name: 'c4', enabled: true }, { id: 8, name: 'c8', enabled: true }, { id: 9, name: 'c9', enabled: true }];
const rule = (id, alert_type, cluster_id) => ({ id, name: `${alert_type} - c${cluster_id}`, alert_type, cluster_id, cluster_name: 'c' + cluster_id, severity: 'warning', threshold: 1,
  config: { mode: 'simple', warning: 70 }, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, enabled: false, email_enabled: false, open_trigger_count: 0 });
let configs = [];
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });

const dom = new JSDOM(html, { runScripts: 'dangerously', url: 'http://localhost/', beforeParse(w) {
  w.Element.prototype.scrollIntoView = () => {};
  w.mountUserBadge = () => {}; w.fetchCurrentUser = async () => null;
  w.fetch = async (url, opts = {}) => {
    const method = opts.method || 'GET';
    if (url.endsWith('/settings/clusters')) return json({ clusters });
    if (url.includes('/alerts/triggers')) return json({ triggers: [], total_count: 0 });
    if (url.endsWith('/alerts/configs') && method === 'GET') return json({ configs });
    const body = JSON.parse(opts.body);
    calls.push({ path: url.replace(/^.*?(\/alerts)/, '$1'), body });
    // Copy reply: every target created (the API would skip existing ones itself).
    if (url.endsWith('/alerts/configs/copy')) return json({ preview: false, created: body.target_cluster_ids.map(id => ({ id: 200 + id, cluster_id: id })), skipped: [] });
    return json({ config: { id: 100, cluster_id: body.cluster_id, cluster_name: 'c' + body.cluster_id } });
  };
}});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '\n    ' + extra : ''}`); };
const fmt = sent => sent.map(c => `POST ${c.path} cluster_id=${c.body.cluster_id ?? '-'} enabled=${c.body.enabled ?? '-'}${c.body.target_cluster_ids ? ' config_id=' + c.body.config_id + ' targets=' + JSON.stringify(c.body.target_cluster_ids) : ''}`).join('\n    ') || '(no request)';
async function run(type, existing) {
  configs = existing; await w.eval('loadConfigs()');
  $('btn-new-alert').click(); $('af-name').value = 'every';
  $('af-alert-type').value = type; $('af-alert-type').dispatchEvent(new w.Event('change'));
  $('af-tier-warning').value = '70';
  $('af-all-clusters').checked = true; $('af-all-clusters').dispatchEvent(new w.Event('change'));
  const n = calls.length; $('btn-save-alert').click(); await flush();
  return calls.slice(n);
}
const cmsg = () => $('configs-msg').textContent, fmsg = () => $('alert-form-msg').textContent;

(async () => {
  await flush();
  let s = await run('broker.cpu_pct', [rule(20, 'broker.cpu_pct', 4), rule(21, 'broker.heap_pct', 8)]);
  check('cluster 4 has CPU: first POST on 8, copy to [9], 4 counted as skipped',
    s.length === 2 && s[0].path === '/alerts/configs' && s[0].body.cluster_id === 8 && s[0].body.enabled === false
    && s[1].path === '/alerts/configs/copy' && JSON.stringify(s[1].body.target_cluster_ids) === '[9]' && s[1].body.config_id === 100
    && cmsg() === '✓ Created 2 rule(s), 1 skipped', fmt(s) + `\n    configs-msg='${cmsg()}'`);

  s = await run('broker.heap_pct', [rule(30, 'broker.heap_pct', 4), rule(31, 'broker.heap_pct', 8), rule(32, 'broker.heap_pct', 9)]);
  check('all clusters have heap: message, no request, form stays open',
    s.length === 0 && fmsg() === '✗ Every cluster already has this rule' && $('alert-form-msg').classList.contains('err') && !$('alert-form-card').classList.contains('hidden'), fmt(s) + `\n    msg='${fmsg()}'`);
  $('btn-cancel-alert').click();

  s = await run('broker.cpu_pct', [rule(20, 'broker.cpu_pct', 4), rule(22, 'broker.cpu_pct', 8)]);
  check('only 9 left: one POST on 9, no copy, "Created 1 rule(s), 2 skipped"',
    s.length === 1 && s[0].path === '/alerts/configs' && s[0].body.cluster_id === 9 && s[0].body.enabled === false
    && cmsg() === '✓ Created 1 rule(s), 2 skipped' && $('alert-form-card').classList.contains('hidden'), fmt(s) + `\n    configs-msg='${cmsg()}'`);

  s = await run('broker.heap_pct', [rule(20, 'broker.cpu_pct', 4), rule(22, 'broker.cpu_pct', 8)]);
  check('no cluster has heap: as before (first on 4, copy to [8, 9])',
    s.length === 2 && s[0].body.cluster_id === 4 && JSON.stringify(s[1].body.target_cluster_ids) === '[8,9]' && cmsg() === '✓ Created 3 rule(s), 0 skipped', fmt(s) + `\n    configs-msg='${cmsg()}'`);
  console.log(fails ? `${fails} FAILED` : 'ALL PASS');
})();
