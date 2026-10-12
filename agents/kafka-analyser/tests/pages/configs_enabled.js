const { TEAMS_HTML } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');
const calls = [];
let clusters = [{ id: 3, name: 'c3', enabled: false }, { id: 4, name: 'c4', enabled: true }, { id: 8, name: 'c8', enabled: true }];
const rule = (id, alert_type, cluster_id) => ({ id, name: `${alert_type} - c${cluster_id}`, alert_type, cluster_id, cluster_name: 'c' + cluster_id, severity: 'warning', threshold: 1,
  config: { mode: 'simple', warning: 70 }, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, enabled: false, email_enabled: false, open_trigger_count: 0 });
const configs = [rule(20, 'broker.cpu_pct', 4), rule(30, 'broker.heap_pct', 3)];
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });
const dom = new JSDOM(html, { runScripts: 'dangerously', url: 'http://localhost/', beforeParse(w) {
  w.Element.prototype.scrollIntoView = () => {};
  w.mountUserBadge = () => {}; w.fetchCurrentUser = async () => null;
  w.fetch = async (url, opts = {}) => {
    const method = opts.method || 'GET';
    if (url.endsWith('/settings/clusters')) return json({ clusters });
    if (url.includes('/alerts/triggers')) return json({ triggers: [], total_count: 0 });
    if (url.endsWith('/alerts/configs') && method === 'GET') return json({ configs });
    const body = JSON.parse(opts.body); calls.push({ path: url.replace(/^.*?(\/alerts)/, '$1'), body });
    if (url.endsWith('/copy')) return json({ preview: false, created: body.target_cluster_ids.map(id => ({ id: 200 + id, cluster_id: id })), skipped: [] });
    return json({ config: { id: 100, cluster_id: body.cluster_id, cluster_name: 'c' + body.cluster_id } });
  };
}});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '\n    ' + extra : ''}`); };
const opts = () => [...$('af-cluster').options].map(o => `${o.value || "''"}=${o.textContent}`).join(' | ');
(async () => {
  await flush();
  check('dropdown: All clusters + 4 and 8 only, no "(disabled)" label', [...$('af-cluster').options].map(o => o.value).join() === ',4,8' && !opts().includes('disabled'), opts());

  $('btn-new-alert').click(); $('af-name').value = 'every';
  $('af-alert-type').value = 'broker.heap_pct'; $('af-alert-type').dispatchEvent(new w.Event('change'));
  $('af-tier-warning').value = '70';
  $('af-all-clusters').checked = true; $('af-all-clusters').dispatchEvent(new w.Event('change'));
  $('btn-save-alert').click(); await flush();
  check('create for every cluster (heap; 3 already has heap but is disabled): POST on 4, copy to [8] only',
    calls.length === 2 && calls[0].body.cluster_id === 4 && JSON.stringify(calls[1].body.target_cluster_ids) === '[8]' && $('configs-msg').textContent === '✓ Created 2 rule(s), 0 skipped',
    calls.map(c => `${c.path} ${c.path.endsWith('copy') ? 'targets=' + JSON.stringify(c.body.target_cluster_ids) : 'cluster_id=' + c.body.cluster_id}`).join(' ; ') + ` | ${$('configs-msg').textContent}`);

  d.querySelector('.config-copy[data-id="20"]').click();
  const targets = [...$('copy-clusters').querySelectorAll('.copy-target')].map(b => b.value);
  check('Copy panel for CPU rule on 4: targets [8] only (3 disabled not offered)', targets.join() === '8', 'targets=' + JSON.stringify(targets));
  $('btn-copy-cancel').click();

  w.eval('openEditForm(_alertConfigs.find(c => c.id === 30))');
  const sel = $('af-cluster');
  check('edit heap rule on disabled cluster 3: cluster 3 shown via temporary option', sel.value === '3' && sel.selectedOptions[0].dataset.temp === '1' && sel.selectedOptions[0].textContent === 'c3', opts());
  $('name' in {} ? '' : 'af-name').value = 'renamed';
  const n = calls.length; $('btn-save-alert').click(); await flush();
  check('untouched save of that rule: PUT without cluster_id', calls.length === n + 1 && !('cluster_id' in calls[n].body), JSON.stringify(calls[n] && calls[n].body));
  check('Show filter still lists cluster 3 (has rules)', [...$('configs-cluster-filter').options].some(o => o.value === '3'), [...$('configs-cluster-filter').options].map(o => o.value || "''").join(','));

  clusters = [{ id: 3, name: 'c3', enabled: false }];
  await w.eval('loadClusterOptions()');
  $('btn-new-alert').click();
  check('no enabled cluster, CLOSE_WAIT form: hint', $('af-cluster-hint').textContent === 'No enabled clusters — only "All clusters" is available.' && [...$('af-cluster').options].length === 1, `hint='${$('af-cluster-hint').textContent}'`);
  $('af-alert-type').value = 'broker.cpu_pct'; $('af-alert-type').dispatchEvent(new w.Event('change'));
  check('no enabled cluster, CPU form: hint', $('af-cluster-hint').textContent === 'No enabled clusters — this metric needs a cluster, so a new rule cannot be saved.', `hint='${$('af-cluster-hint').textContent}'`);
  console.log(fails ? `${fails} FAILED` : 'ALL PASS');
})();
