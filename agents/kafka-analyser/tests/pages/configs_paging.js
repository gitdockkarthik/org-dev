const { TEAMS_HTML } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');

const calls = [];
const clusters = [{ id: 4, name: 'c4', enabled: true }, { id: 8, name: 'c8', enabled: true }, { id: 9, name: 'c9', enabled: true }];
const base = { severity: 'warning', threshold: 1, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, enabled: true, email_enabled: false, open_trigger_count: 0, min_rf: null };
const tiers = { mode: 'simple', warning: 70 };
let configs = [];
let id = 300;
// Ids handed out in reverse of the expected order, so sorting by id alone is wrong.
for (const [type, name] of [['cluster.urp_total', 'URP'], ['cluster.rf_below_min', 'RF'], ['broker.heap_pct', 'Heap'], ['broker.cpu_pct', 'CPU']])
  for (const [cid, cname] of [[9, 'c9'], [8, 'c8'], [4, 'c4']])
    configs.push({ ...base, id: id--, name: `${name} - ${cname}`, alert_type: type, cluster_id: cid, cluster_name: cname,
                   config: type === 'cluster.rf_below_min' ? { ...tiers, min_rf: 3 } : tiers, min_rf: type === 'cluster.rf_below_min' ? 3 : null });
configs.push({ ...base, id: 50, name: 'CW all B', alert_type: 'close_wait_spike', cluster_id: null, cluster_name: 'All clusters', config: { threshold: 1 } });
configs.push({ ...base, id: 40, name: 'CW all A', alert_type: 'close_wait_spike', cluster_id: null, cluster_name: 'All clusters', config: { threshold: 1 } });
configs.push({ ...base, id: 60, name: 'CW c8', alert_type: 'close_wait_spike', cluster_id: 8, cluster_name: 'c8', config: { threshold: 1 } });
configs.push({ ...base, id: 70, name: 'BU all', alert_type: 'broker_unreachable', cluster_id: null, cluster_name: 'All clusters', config: { threshold: 1 } });
configs.push({ ...base, id: 71, name: 'BU all 2', alert_type: 'broker_unreachable', cluster_id: null, cluster_name: 'All clusters', config: { threshold: 1 } });
configs.push({ ...base, id: 80, name: 'CW c4', alert_type: 'close_wait_spike', cluster_id: 4, cluster_name: 'c4', config: { threshold: 1 } });
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });

const dom = new JSDOM(html, {
  runScripts: 'dangerously',
  url: 'http://localhost/',
  beforeParse(w) {
    w.Element.prototype.scrollIntoView = () => {};
    w.mountUserBadge = () => {};
    w.fetchCurrentUser = async () => null;
    w.confirm = () => true;
    w.fetch = async (url, opts = {}) => {
      const method = opts.method || 'GET';
      calls.push({ method, url, body: opts.body ? JSON.parse(opts.body) : null });
      if (url.endsWith('/settings/clusters')) return json({ clusters });
      if (url.includes('/alerts/triggers')) return json({ triggers: [], total_count: 0 });
      if (url.endsWith('/alerts/configs') && method === 'GET') return json({ configs });
      const m = url.match(/\/alerts\/configs\/(\d+)$/);
      if (m && method === 'DELETE') { configs = configs.filter(c => c.id !== Number(m[1])); return json({ ok: true }); }
      if (m && method === 'PUT') { const c = configs.find(x => x.id === Number(m[1])); Object.assign(c, calls.at(-1).body); return json({ config: c }); }
      return json({ config: {} });
    };
  },
});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '  ' + extra : ''}`); };
const rows = () => [...d.querySelectorAll('#configs-body tr')];
const ids = () => rows().map(tr => tr.querySelector('button.config-edit')).filter(Boolean).map(b => Number(b.dataset.id));
const label = () => $('configs-page-label').textContent;
const prevDis = () => $('btn-configs-prev').disabled, nextDis = () => $('btn-configs-next').disabled;
const pagerHidden = () => $('configs-pager').classList.contains('hidden');
const setSel = (sel, v) => { $(sel).value = v; $(sel).dispatchEvent(new w.Event('change')); };
const state = () => `ids=${JSON.stringify(ids())} label='${label()}' prev=${prevDis()} next=${nextDis()}`;
const configRequests = () => calls.filter(c => c.url.includes('/alerts/configs')).length;

(async () => {
  await flush();
  // Controls and markup
  check('page size select: options 10 and 25, default 10', JSON.stringify([...$('configs-page-size').options].map(o => o.value)) === '["10","25"]' && $('configs-page-size').value === '10');
  // Existing styles plus the hook class shared by the top and bottom pagers.
  check('controls use existing styles', $('btn-configs-prev').className === 'btn btn-secondary btn-sm configs-prev' && $('btn-configs-next').className === 'btn btn-secondary btn-sm configs-next'
        && $('configs-page-label').className === 'hint configs-page-label' && !pagerHidden());

  // Sort order
  const p1 = ids();
  check('page 1: 10 rows, label, Previous disabled, Next enabled', p1.length === 10 && label() === 'Showing 1-10 of 18' && prevDis() && !nextDis(), state());
  const typeOf = idv => configs.find(c => c.id === idv);
  const typeLabels = { 'broker.cpu_pct': 'Broker CPU %', 'broker.heap_pct': 'Broker heap %', broker_unreachable: 'Broker reachability (live check)', close_wait_spike: 'CLOSE_WAIT spike', 'cluster.rf_below_min': 'Topics below minimum RF', 'cluster.urp_total': 'Under-replicated partitions' };
  // Expected order written out by hand: type label, then cluster name, then id.
  const expected = [
    300 - 11, 300 - 10, 300 - 9,   // Broker CPU %: c4 c8 c9
    300 - 8, 300 - 7, 300 - 6,     // Broker heap %: c4 c8 c9
    70, 71,                         // Broker reachability: All clusters, by id
    40, 50,                         // CLOSE_WAIT spike: All clusters 40, 50
    80, 60,                         // CLOSE_WAIT spike: c4, c8
    300 - 5, 300 - 4, 300 - 3,     // Topics below minimum RF: c4 c8 c9
    300 - 2, 300 - 1, 300,         // Under-replicated partitions: c4 c8 c9
  ];
  $('btn-configs-next').click();
  const p2 = ids();
  check('page 2: 8 rows, label, Next disabled, Previous enabled', p2.length === 8 && label() === 'Showing 11-18 of 18' && !prevDis() && nextDis(), state());
  check('sort: type label, then cluster name, then id', JSON.stringify([...p1, ...p2]) === JSON.stringify(expected),
        [...p1, ...p2].map(i => `${i}:${typeLabels[typeOf(i).alert_type].split(' ')[0]}/${typeOf(i).cluster_name}`).join(' '));
  const before = configRequests();
  $('btn-configs-next').click();
  check('Next on the last page stays (no request)', JSON.stringify(ids()) === JSON.stringify(p2) && configRequests() === before);
  $('btn-configs-prev').click();
  check('Previous returns to page 1 without a request', JSON.stringify(ids()) === JSON.stringify(p1) && configRequests() === before, state());
  $('btn-configs-prev').click();
  check('Previous on page 1 stays', JSON.stringify(ids()) === JSON.stringify(p1));

  // Page size
  $('btn-configs-next').click();
  setSel('configs-page-size', '25');
  check('size 25: all 18 on one page, both disabled', ids().length === 18 && label() === 'Showing 1-18 of 18' && prevDis() && nextDis(), state());
  setSel('configs-page-size', '10');
  check('size back to 10: page 1', JSON.stringify(ids()) === JSON.stringify(p1) && label() === 'Showing 1-10 of 18');

  // Filter
  $('btn-configs-next').click();
  setSel('configs-cluster-filter', '4');
  check('filter cluster 4 from page 2 -> page 1, 5 rules', ids().length === 5 && label() === 'Showing 1-5 of 5' && prevDis() && nextDis(), state());
  setSel('configs-cluster-filter', '');
  check('filter back to all -> page 1', label() === 'Showing 1-10 of 18');
  setSel('configs-cluster-filter', 'general');
  // General = general types (by type, so CLOSE_WAIT rules on c4/c8 count too).
  check('general filter: 6 rules', ids().length === 6 && label() === 'Showing 1-6 of 6', state());
  // Filter that matches nothing: cluster 9 selected, then rendered from a
  // list without its rules (a reload would rebuild the filter options).
  setSel('configs-cluster-filter', '9');
  w.renderConfigs(w.eval('_alertConfigs').filter(c => c.cluster_id !== 9));
  check('filter matches nothing: existing empty row, "Showing 0 of N", pager shown, both disabled',
        rows().length === 1 && rows()[0].textContent === 'No rules match this filter.' && rows()[0].querySelector('td').colSpan === 10
        && label() === 'Showing 0 of 14' && !pagerHidden() && prevDis() && nextDis(), `rows='${rows()[0].textContent}' colspan=${rows()[0].querySelector('td').colSpan} label='${label()}' hidden=${pagerHidden()} prev=${prevDis()} next=${nextDis()}`);
  setSel('configs-cluster-filter', '');
  await w.loadConfigs();

  // Reload and toggle keep the page
  $('btn-configs-next').click();
  await w.loadConfigs();
  check('reload keeps page 2', label() === 'Showing 11-18 of 18', state());
  const toggle = d.querySelector('#configs-body .config-enable-toggle');
  const tid = Number(toggle.dataset.id);
  toggle.checked = false; toggle.dispatchEvent(new w.Event('change')); await flush();
  check(`enable toggle on page 2 (rule ${tid}) PUTs and keeps page 2`, calls.some(c => c.method === 'PUT' && c.url.endsWith('/' + tid) && c.body.enabled === false) && label() === 'Showing 11-18 of 18', state());
  const etoggle = d.querySelector('#configs-body .config-email-toggle');
  etoggle.checked = true; etoggle.dispatchEvent(new w.Event('change')); await flush();
  check('email toggle keeps page 2', label() === 'Showing 11-18 of 18');

  // Edit and Copy on a page 2 row
  const rfRow = rows().find(tr => tr.textContent.includes('RF - c8'));
  rfRow.querySelector('button.config-edit').click();
  check('Edit on page 2 opens that rule', $('af-name').value === 'RF - c8' && $('af-alert-type').value === 'cluster.rf_below_min' && $('af-min-rf').value === '3' && !$('alert-form-card').classList.contains('hidden'));
  $('af-name').value = 'RF - c8 renamed';
  $('btn-save-alert').click(); await flush();
  check('saving the edit keeps page 2', calls.some(c => c.method === 'PUT' && c.body.name === 'RF - c8 renamed') && label() === 'Showing 11-18 of 18', state());
  rows().find(tr => tr.textContent.includes('URP - c9')).querySelector('button.config-copy').click();
  check('Copy on page 2 opens the panel for that rule', $('copy-source').textContent === 'Source: URP - c9 (Under-replicated partitions) on c9' && !$('copy-panel').classList.contains('hidden'), $('copy-source').textContent);
  check('opening copy keeps page 2', label() === 'Showing 11-18 of 18');

  // Delete the last rule on the last page
  configs = configs.slice(0, 11);
  await w.loadConfigs();
  check('11 rules on page 2 -> 1 row', ids().length === 1 && label() === 'Showing 11-11 of 11', state());
  rows()[0].querySelector('button.config-delete').click(); await flush(); await flush();
  check('after deleting it: back to page 1 with 10 rows, Next disabled', ids().length === 10 && label() === 'Showing 1-10 of 10' && prevDis() && nextDis(), state());

  // No rules at all
  configs = [];
  await w.loadConfigs();
  check('no rules: controls hidden, original empty row', pagerHidden() && rows()[0].textContent.startsWith('No alert rules configured yet.') && rows()[0].querySelector('td').colSpan === 10);

  console.log(fails ? `${fails} FAILED` : 'ALL PASSED');
  process.exit(fails ? 1 : 0);
})();
