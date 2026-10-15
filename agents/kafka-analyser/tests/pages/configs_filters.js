const { TEAMS_HTML } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');
const clusters = [{ id: 4, name: 'c4', enabled: true }, { id: 8, name: 'c8', enabled: true }, { id: 9, name: 'c9', enabled: true }];
const base = { severity: 'warning', threshold: 1, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, email_enabled: false, open_trigger_count: 0, min_rf: null };
const LABEL = { 'broker.cpu_pct': 'Broker CPU % ', 'broker.heap_pct': 'Broker heap % ', 'cluster.urp_total': 'URP ', 'cluster.leader_skew': 'Skew ',
                'cluster.data_gb_max': 'Data max ', 'cluster.data_spread_pct': 'Data spread ', 'cluster.msg_rate_in': 'Rate ', 'cluster.zk_ensemble': 'ZK ' };
// 34 cluster rules (types x clusters, minus some) + 2 general = 36; ids 3, 5 general and enabled, 20 enabled.
let configs = [], id = 20;
for (const t of Object.keys(LABEL)) for (const c of clusters) {
  if (configs.length >= 34) break;
  if (t === 'cluster.zk_ensemble' && c.id !== 9) continue;
  configs.push({ ...base, id, name: LABEL[t] + '- ' + c.name, alert_type: t, cluster_id: c.id, cluster_name: c.name, enabled: id === 20, config: { mode: 'simple', warning: 1, critical: 2 } }); id++;
}
while (configs.length < 34) { configs.push({ ...base, id, name: 'Extra URP <b>' + id + '</b> - c4', alert_type: 'cluster.urp_total', cluster_id: 4, cluster_name: 'c4', enabled: false, config: { mode: 'simple', warning: 1 } }); id++; }
configs.push({ ...base, id: 3, name: 'CLOSE_WAIT spike', alert_type: 'close_wait_spike', cluster_id: null, cluster_name: null, enabled: true, threshold: 5, config: null });
configs.push({ ...base, id: 5, name: 'Broker reachability', alert_type: 'broker_unreachable', cluster_id: null, cluster_name: null, enabled: true, config: null });
const calls = [];
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });
const dom = new JSDOM(html, {
  runScripts: 'dangerously', url: 'http://localhost/',
  beforeParse(w) {
    w.Element.prototype.scrollIntoView = () => {};
    w.mountUserBadge = () => {}; w.fetchCurrentUser = async () => null;
    w.fetch = async (url, opts = {}) => {
      const method = opts.method || 'GET', body = opts.body ? JSON.parse(opts.body) : null;
      if (url.endsWith('/settings/clusters')) return json({ clusters });
      if (url.includes('/alerts/triggers')) return json({ triggers: [], total_count: 0 });
      calls.push({ method, url, body });
      if (url.endsWith('/alerts/configs') && method === 'GET') return json({ configs: JSON.parse(JSON.stringify(configs)) });
      const m = url.match(/\/alerts\/configs\/(\d+)$/);
      if (m && method === 'PUT') { const c = configs.find(x => x.id === +m[1]); Object.assign(c, body); return json({ config: c }); }
      if (m && method === 'DELETE') { configs = configs.filter(x => x.id !== +m[1]); return json({ ok: true }); }
      return json({ config: { id: 999 } });
    };
  },
});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '  ' + extra : ''}`); };
const sel = (id, v) => { $(id).value = v; $(id).dispatchEvent(new w.Event('change')); };
const type = v => { $('configs-name-search').value = v; $('configs-name-search').dispatchEvent(new w.Event('input')); };
const ids = () => [...d.querySelectorAll('#configs-body button.config-edit')].map(b => +b.dataset.id);
const label = () => $('configs-page-label').textContent, count = () => $('configs-count').textContent;
const row = id => [...d.querySelectorAll('#configs-body tr')].find(tr => tr.querySelector(`button.config-edit[data-id="${id}"]`));
const allRows = async () => { sel('configs-page-size', '25'); const a = ids(); $('btn-configs-next').click(); const b = ids(); $('btn-configs-prev').click(); sel('configs-page-size', '10'); return [...new Set([...a, ...b])]; };
const reset = () => { sel('configs-cluster-filter', ''); sel('configs-type-filter', ''); sel('configs-state-filter', ''); $('btn-configs-name-clear').click(); };
const configReqs = () => calls.filter(c => c.method === 'GET').length;

(async () => {
  await flush();
  check('36 rules loaded; count "36 rules, 3 active"', configs.length === 36 && count() === '36 rules, 3 active', count());
  const order = [...d.querySelector('.configs-filters').querySelectorAll('select, input')].map(e => e.id);
  check('one filter row: cluster, type, state, name search, per page', JSON.stringify(order) === '["configs-cluster-filter","configs-type-filter","configs-state-filter","configs-name-search","configs-page-size"]', JSON.stringify(order));
  check('Clear button sits next to the search', $('btn-configs-name-clear').previousElementSibling === $('configs-name-search') && $('btn-configs-name-clear').type === 'button');
  check('count line above the table', !!($('configs-count').compareDocumentPosition($('configs-body')) & w.Node.DOCUMENT_POSITION_FOLLOWING));
  check('filter row wraps (flex-wrap: wrap)', w.getComputedStyle(d.querySelector('.configs-filters')).flexWrap === 'wrap');

  const tOpts = [...$('configs-type-filter').options].map(o => [o.value, o.textContent]);
  const present = [...new Set(configs.map(c => c.alert_type))];
  check('type options: "All types" + only present types, sorted by label', tOpts[0][0] === '' && tOpts[0][1] === 'All types' && tOpts.length === present.length + 1
        && tOpts.slice(1).every(([v]) => present.includes(v)) && JSON.stringify(tOpts.slice(1).map(o => o[1])) === JSON.stringify(tOpts.slice(1).map(o => o[1]).slice().sort((a, b) => a.localeCompare(b)))
        && !tOpts.some(([v]) => v === 'cluster.rf_below_min'), JSON.stringify(tOpts.map(o => o[1])));
  check('state options All / Active / Inactive', [...$('configs-state-filter').options].map(o => o.value + '=' + o.textContent).join() === '=All,enabled=Active,disabled=Inactive');

  const r0 = configReqs();
  // each filter alone
  sel('configs-cluster-filter', '9'); let got = await allRows(), want = configs.filter(c => c.cluster_id === 9).map(c => c.id);
  check(`cluster 9 alone: ${want.length} rules, count "Showing ${want.length} of 36 rules"`, JSON.stringify(got.sort()) === JSON.stringify(want.sort()) && count() === `Showing ${want.length} of 36 rules`, count()); reset();
  sel('configs-type-filter', 'cluster.urp_total'); got = await allRows(); want = configs.filter(c => c.alert_type === 'cluster.urp_total').map(c => c.id);
  check(`type URP alone: ${want.length} rules`, JSON.stringify(got.sort()) === JSON.stringify(want.sort()) && count() === `Showing ${want.length} of 36 rules`, count()); reset();
  sel('configs-state-filter', 'enabled'); got = await allRows();
  check('state Active alone: 3, 5, 20', JSON.stringify(got.sort((a, b) => a - b)) === '[3,5,20]' && count() === 'Showing 3 of 36 rules', count()); reset();
  sel('configs-state-filter', 'disabled'); check('state Inactive alone: 33', count() === 'Showing 33 of 36 rules' && label() === 'Showing 1-10 of 33', count()); reset();
  type('  bRoKeR cpu  '); got = await allRows(); want = configs.filter(c => c.name.toLowerCase().includes('broker cpu')).map(c => c.id);
  check(`name "  bRoKeR cpu  " alone (case-insensitive, trimmed): ${want.length}`, want.length === 3 && JSON.stringify(got.sort()) === JSON.stringify(want.sort()) && count() === 'Showing 3 of 36 rules', count());
  type('<b>'); got = await allRows();
  check('name "<b>" matches the escaped names literally', got.length === configs.filter(c => c.name.includes('<b>')).length && got.length > 0 && !d.querySelector('#configs-body b'), JSON.stringify(got));
  type('   '); check('whitespace-only search is not a filter', count() === '36 rules, 3 active', count()); reset();
  // combined
  sel('configs-cluster-filter', '4'); sel('configs-type-filter', 'broker.cpu_pct'); sel('configs-state-filter', 'enabled'); type('cpu');
  check('all four combined: only rule 20', JSON.stringify(ids()) === '[20]' && count() === 'Showing 1 of 36 rules' && label() === 'Showing 1-1 of 1', `${ids()} | ${count()} | ${label()}`);
  sel('configs-state-filter', 'disabled');
  check('nothing matches: message row and "Showing 0 of 36 rules", pager "Showing 0 of 36"', $('configs-body').textContent.trim() === 'No rules match this filter.' && count() === 'Showing 0 of 36 rules' && label() === 'Showing 0 of 36' && $('btn-configs-next').disabled && $('btn-configs-prev').disabled, `${count()} | ${label()}`);
  sel('configs-cluster-filter', 'general'); sel('configs-type-filter', ''); sel('configs-state-filter', ''); $('btn-configs-name-clear').click();
  check('general filter still works with the others: 3, 5', JSON.stringify(ids().sort((a, b) => a - b)) === '[3,5]' && $('configs-name-search').value === '', count()); reset();
  check('no filter changes made a request', configReqs() === r0, `${configReqs() - r0}`);

  // page resets
  for (const [lab, act] of [['cluster', () => sel('configs-cluster-filter', '4')], ['type', () => sel('configs-type-filter', 'cluster.urp_total')],
                            ['state', () => sel('configs-state-filter', 'disabled')], ['name', () => type('c')], ['Clear', () => $('btn-configs-name-clear').click()]]) {
    if (lab === 'Clear') type('-'); else reset();
    $('btn-configs-next').click();
    const was = label(); act();
    check(`changing ${lab} returns to page 1`, was.startsWith('Showing 11-') && label().startsWith('Showing 1-'), `${was} -> ${label()}`);
  }
  reset();
  check('no filter: count back to "36 rules, 3 active", pager 1-10 of 36', count() === '36 rules, 3 active' && label() === 'Showing 1-10 of 36');

  // Edit / Copy on a filtered row
  sel('configs-cluster-filter', '8'); type('heap');
  const hid = ids()[0], hc = configs.find(c => c.id === hid);
  row(hid).querySelector('button.config-edit').click();
  check(`Edit on filtered row opens rule ${hid}`, $('af-name').value === hc.name && $('af-cluster').value === '8' && !$('alert-form-card').classList.contains('hidden'), $('af-name').value);
  $('btn-cancel-alert').click();
  row(hid).querySelector('button.config-copy').click();
  check('Copy on filtered row opens the panel for it', $('copy-source').textContent.startsWith(`Source: ${hc.name} `) && !$('copy-panel').classList.contains('hidden'), $('copy-source').textContent);
  reset();

  // reload keeps filters and page
  sel('configs-state-filter', 'disabled'); type('- c'); $('btn-configs-next').click();
  const before = [count(), label(), JSON.stringify(ids())];
  const tid = ids()[0];
  check('Inactive + "- c" on page 2: 33 rules', before[0] === 'Showing 33 of 36 rules' && before[1].startsWith('Showing 11-'), before.join(' | '));
  row(tid).querySelector('.config-email-toggle').click(); await flush();
  check(`email on for rule ${tid} (Teams off) under "Inactive": email-only is active, it leaves the list, filters and page 2 kept`,
        calls.some(c => c.method === 'PUT' && c.url.endsWith('/' + tid) && c.body.email_enabled === true) && !row(tid)
        && $('configs-state-filter').value === 'disabled' && $('configs-name-search').value === '- c'
        && count() === 'Showing 32 of 36 rules' && label().startsWith('Showing 11-'), `${count()} | ${label()}`);
  const tid2 = ids()[0];
  row(tid2).querySelector('.config-enable-toggle').click(); await flush();
  check(`Teams on for rule ${tid2} under "Inactive": it leaves the list, page 2 kept, count updates`, !row(tid2) && count() === 'Showing 31 of 36 rules' && label().startsWith('Showing 11-') && $('configs-state-filter').value === 'disabled', `${count()} | ${label()}`);
  sel('configs-state-filter', 'enabled'); got = await allRows();
  check(`Active + "- c" lists rule 20, the email-only rule ${tid} and the Teams-only rule ${tid2}`,
        JSON.stringify(got.sort((a, b) => a - b)) === JSON.stringify([20, tid, tid2].sort((a, b) => a - b)) && count() === 'Showing 3 of 36 rules', `${JSON.stringify(got)} | ${count()}`);
  reset(); check('active count now 5 (email-only rule counted)', count() === '36 rules, 5 active', count());

  // type options rebuilt on reload, selection kept / dropped
  sel('configs-type-filter', 'cluster.zk_ensemble');
  const zk = configs.find(c => c.alert_type === 'cluster.zk_ensemble').id;
  row(zk).querySelector('.config-email-toggle').click(); await flush();
  check('type selection kept across a reload', $('configs-type-filter').value === 'cluster.zk_ensemble' && JSON.stringify(ids()) === `[${zk}]`);
  sel('configs-type-filter', ''); check('ZK rule now email-only: active count 6', count() === '36 rules, 6 active', count()); sel('configs-type-filter', 'cluster.zk_ensemble');
  w.confirm = () => true;
  row(zk).querySelector('.config-delete').click(); await flush(); await flush();
  check('deleting the only ZK rule: option removed, back to "All types"', calls.some(c => c.method === 'DELETE' && c.url.endsWith('/' + zk)) && $('configs-type-filter').value === ''
        && ![...$('configs-type-filter').options].some(o => o.value === 'cluster.zk_ensemble') && count() === '35 rules, 5 active', count());

  console.log(fails ? `${fails} FAILED` : 'ALL PASSED');
  process.exit(fails ? 1 : 0);
})();
