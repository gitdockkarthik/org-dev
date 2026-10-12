const { TEAMS_HTML, alertTypes } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');

let fetches = 0;
const clusters = [{ id: 4, name: 'c4', enabled: true }, { id: 8, name: '<b>c8</b>', enabled: true }, { id: 9, name: 'c9 (no rules)', enabled: true }];
const cfg = (id, name, alert_type, cluster_id, cluster_name) => ({ id, name, alert_type, cluster_id, cluster_name, severity: 'warning', threshold: 1,
  config: alert_type.startsWith('broker.') ? { mode: 'simple', warning: 70 } : { threshold: 1 }, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, enabled: true, email_enabled: false, open_trigger_count: 0 });
let configs = [cfg(3, 'Broker unreachable', 'broker_unreachable', null, 'All clusters'), cfg(5, 'CLOSE_WAIT spike', 'close_wait_spike', null, 'All clusters'),
  cfg(20, 'Broker CPU % - c4', 'broker.cpu_pct', 4, 'c4'), cfg(21, 'Broker heap % - c4', 'broker.heap_pct', 4, 'c4'), cfg(30, 'Broker CPU % - c8', 'broker.cpu_pct', 8, '<b>c8</b>')];
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });

const dom = new JSDOM(html, { runScripts: 'dangerously', url: 'http://localhost/', beforeParse(w) {
  w.Element.prototype.scrollIntoView = () => {};
  w.mountUserBadge = () => {}; w.fetchCurrentUser = async () => null;
  w.fetch = async (url, opts = {}) => {
    fetches++;
    if (url.endsWith('/settings/clusters')) return json({ clusters });
    if (url.includes('/alerts/triggers')) return json({ triggers: [], total_count: 0 });
    if (url.endsWith('/alerts/configs')) return json({ configs });
    return json({});
  };
}});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '\n    ' + extra : ''}`); };
const sel = () => $('configs-cluster-filter');
const opts = () => [...sel().options].map(o => `${o.value || "''"}=${o.textContent}`).join(' | ');
const rows = () => [...$('configs-body').querySelectorAll('.config-edit')].map(b => Number(b.dataset.id));
const setFilter = v => { sel().value = v; sel().dispatchEvent(new w.Event('change')); };

(async () => {
  await flush();
  check('options: two fixed + clusters with rules only (no c9), labels escaped', [...sel().options].map(o => o.value).join() === ',general,4,8' && !sel().querySelector('b'), opts() + `  [innerHTML of 8: ${sel().options[3].innerHTML}]`);
  const f0 = fetches;
  // Rows are sorted by type label (the page's ALERT_TYPES), then cluster name, then id.
  const labelOf = Object.fromEntries(alertTypes(w).map(t => [t.value, t.label]));
  const byId = Object.fromEntries(configs.map(c => [c.id, c]));
  const ordered = ids => ids.slice().sort((a, b) => labelOf[byId[a].alert_type].localeCompare(labelOf[byId[b].alert_type])
    || String(byId[a].cluster_name).localeCompare(String(byId[b].cluster_name)) || a - b);
  const expect = { '': ordered([3, 5, 20, 21, 30]), general: ordered([3, 5]), '4': ordered([20, 21]), '8': ordered([30]) };
  for (const [v, ids] of Object.entries(expect)) {
    setFilter(v);
    check(`filter '${v}' shows rows ${ids}`, JSON.stringify(rows()) === JSON.stringify(ids), 'rows=' + JSON.stringify(rows()));
  }
  check('filter changes made no request', fetches === f0, `requests=${fetches - f0}`);

  // Edit and Copy on a filtered row (filter 8, row 30)
  setFilter('8');
  d.querySelector('.config-edit[data-id="30"]').click();
  check('Edit on filtered row opens rule 30', $('af-name').value === 'Broker CPU % - c8' && $('af-cluster').value === '8', `name='${$('af-name').value}' cluster='${$('af-cluster').value}'`);
  $('btn-cancel-alert').click();
  d.querySelector('.config-copy[data-id="30"]').click();
  check('Copy on filtered row opens panel for rule 30 (targets exclude 8)', $('copy-source').textContent.includes('Broker CPU % - c8') && [...$('copy-clusters').querySelectorAll('.copy-target')].map(b => b.value).join() === '4,9', $('copy-source').textContent);
  $('btn-copy-cancel').click();

  // Selection survives reload
  await w.eval('loadConfigs()');
  check('filter 8 survives reload', sel().value === '8' && JSON.stringify(rows()) === '[30]', `value=${sel().value} rows=${JSON.stringify(rows())}`);
  // Cluster 8 loses its rules -> option gone, falls back to All rules
  configs = configs.filter(c => c.id !== 30);
  await w.eval('loadConfigs()');
  check('cluster 8 has no rules after reload: option removed, back to All rules', sel().value === '' && ![...sel().options].some(o => o.value === '8') && rows().length === 4, opts());
  // Filter matching nothing
  setFilter('general');
  configs = configs.filter(c => !['broker_unreachable', 'close_wait_spike'].includes(c.alert_type));
  await w.eval('loadConfigs()');
  check('general filter with no general rules: message row', sel().value === 'general' && $('configs-body').textContent.trim() === 'No rules match this filter.', `'${$('configs-body').textContent.trim()}'`);
  configs = [];
  await w.eval('loadConfigs()');
  check('no rules at all: original empty message', $('configs-body').textContent.includes('No alert rules configured yet'), `'${$('configs-body').textContent.trim()}'`);
  console.log(fails ? `${fails} FAILED` : 'ALL PASS');
})();
