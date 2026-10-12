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

const resetShown = () => !$('btn-configs-reset').classList.contains('hidden');
const state = () => JSON.stringify(['configs-cluster-filter', 'configs-type-filter', 'configs-state-filter', 'configs-name-search', 'configs-page-size'].map(i => $(i).value));
(async () => {
  await flush();
  const x = $('btn-configs-name-clear'), rb = $('btn-configs-reset');
  check('"×" button: same id, compact (no .btn), inside the name box wrapper, labelled', x.textContent === '×' && !x.classList.contains('btn') && x.parentElement.classList.contains('configs-search')
        && x.previousElementSibling === $('configs-name-search') && x.getAttribute('aria-label') === 'Clear name search' && x.type === 'button', x.outerHTML);
  check('"×" sits inside the box (absolute, input padded)', w.getComputedStyle(x).position === 'absolute' && w.getComputedStyle($('configs-name-search')).paddingRight === '1.75rem');
  check('Reset is the last item of the filter row, type button', rb.parentElement.classList.contains('configs-filters') && rb.parentElement.lastElementChild === rb && rb.type === 'button' && rb.textContent === 'Reset filters');
  check('Reset hidden with no filter active', !resetShown() && count() === '36 rules, 3 enabled');

  for (const [lab, act, undo] of [['cluster', () => sel('configs-cluster-filter', '4'), () => sel('configs-cluster-filter', '')],
                                  ['type', () => sel('configs-type-filter', 'cluster.urp_total'), () => sel('configs-type-filter', '')],
                                  ['state', () => sel('configs-state-filter', 'disabled'), () => sel('configs-state-filter', '')],
                                  ['name', () => type('cpu'), () => type('')]]) {
    act(); const on = resetShown(); undo();
    check(`Reset shown when only ${lab} is set, hidden again when it is cleared`, on && !resetShown());
  }
  type('   '); check('whitespace-only name: Reset stays hidden', !resetShown()); type('');

  // Reset restores all four, page 1, keeps Per page, no request
  sel('configs-page-size', '25');
  sel('configs-cluster-filter', '4'); sel('configs-type-filter', 'cluster.urp_total'); sel('configs-state-filter', 'disabled'); type('urp');
  $('btn-configs-next').click();
  const before = [state(), label(), count()];
  const r0 = configReqs();
  rb.click();
  check('Reset: all four filters back to all, Per page stays 25', state() === '["","","","","25"]', `${before[0]} -> ${state()}`);
  check('Reset: page 1, full list, count "36 rules, 3 enabled", Reset hidden, no request',
        label() === 'Showing 1-25 of 36' && count() === '36 rules, 3 enabled' && !resetShown() && configReqs() === r0, `${before[1]} / ${before[2]} -> ${label()} / ${count()}`);
  sel('configs-page-size', '10');
  sel('configs-state-filter', 'disabled'); $('btn-configs-next').click(); $('btn-configs-next').click();
  const p3 = label(); rb.click();
  check('Reset from page 3 -> page 1', p3.startsWith('Showing 21-') && label() === 'Showing 1-10 of 36', `${p3} -> ${label()}`);

  // × clears only the name
  sel('configs-cluster-filter', '4'); sel('configs-type-filter', 'cluster.urp_total'); sel('configs-state-filter', 'disabled'); type('extra');
  const n = count(); $('btn-configs-next').click();
  x.click();
  check('"×" clears only the name; other three kept; page 1; Reset still shown', state() === '["4","cluster.urp_total","disabled","","10"]' && label().startsWith('Showing 1-') && resetShown() && count() !== n, `${state()} | ${count()} | ${label()}`);
  rb.click();
  type('heap'); x.click();
  check('"×" with only a name set: back to the full list, Reset hidden', count() === '36 rules, 3 enabled' && !resetShown());

  // Reset state survives a reload (render on load)
  sel('configs-state-filter', 'disabled');
  const tid = ids()[0]; row(tid).querySelector('.config-email-toggle').click(); await flush();
  check('after a reload with a filter active: Reset still shown', resetShown() && $('configs-state-filter').value === 'disabled');
  rb.click();
  row(ids()[0]).querySelector('.config-email-toggle').click(); await flush();
  check('after a reload with no filter: Reset hidden', !resetShown());

  console.log(fails ? `${fails} FAILED` : 'ALL PASSED');
  process.exit(fails ? 1 : 0);
})();
