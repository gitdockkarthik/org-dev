const { TEAMS_HTML } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');
const clusters = [{ id: 4, name: 'c4', enabled: true }, { id: 8, name: 'c8', enabled: true }, { id: 9, name: 'c9 <i>', enabled: true }];
const configs = [{ id: 20, name: 'Broker CPU % - c4', alert_type: 'broker.cpu_pct', cluster_id: 4, cluster_name: 'c4', severity: 'warning', threshold: 1, send_resolve_card: true, webhook_url: null,
                   cooldown_minutes: 30, enabled: true, email_enabled: false, open_trigger_count: 0, config: { mode: 'simple', warning: 70 }, min_rf: null }];
// 40 firings, newest first, one per 90 min from T0 back; cluster cycles 4/8/9; every 7th open; every 5th recurring; every 6th failed post;
// i = 5, 15, 25, 35 not posted because Teams is off (the rule's toggle or the global switch).
const OFF = ['Teams off for this rule', 'Teams switched off globally'];
const T0 = Date.parse('2026-10-06T08:00:00Z');
const NAMES = ['Broker CPU % - c', 'URP - c', 'ZooKeeper ensemble problems - c', 'Data <b>max</b> - c'];
const all = Array.from({ length: 40 }, (_, i) => {
  const cid = [4, 8, 9][i % 3];
  return { id: 1000 - i, alert_config_id: 20 + (i % 4), config_name: NAMES[i % 4] + cid, cluster_id: cid, cluster_name: cid === 9 ? 'c9 <i>' : 'c' + cid,
           triggered_at: new Date(T0 - i * 90 * 60000).toISOString(), resolved_at: i % 7 === 0 ? null : new Date(T0 - i * 90 * 60000 + 600000).toISOString(),
           is_recurrence: i % 5 === 0, metric_value: String(i), teams_post_success: i % 6 !== 0 && i % 10 !== 5,
           error_detail: i % 6 === 0 ? 'HTTP 400 <bad>' : i % 10 === 5 ? OFF[(i % 20 === 5) ? 0 : 1] : null };
});
let mode = 'ok', total = null;  // total overrides total_count
const reqs = [];
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });
const dom = new JSDOM(html, {
  runScripts: 'dangerously', url: 'http://localhost/',
  beforeParse(w) {
    w.Element.prototype.scrollIntoView = () => {};
    w.mountUserBadge = () => {}; w.fetchCurrentUser = async () => null;
    w.fetch = async (url, opts = {}) => {
      if (url.endsWith('/settings/clusters')) return json({ clusters });
      if (url.endsWith('/alerts/configs')) return json({ configs });
      if (url.includes('/alerts/triggers')) {
        reqs.push(url);
        if (mode === 'error') return json({ detail: 'db down' }, 500);
        if (mode === 'empty') return json({ triggers: [], total_count: 0 });
        const days = +new URL(url, 'http://x').searchParams.get('since_days');
        const rows = all.filter(t => Date.parse(t.triggered_at) >= T0 - days * 86400000 + 3600000 * 0);
        const rows2 = days === 3 ? rows.filter(t => Date.parse(t.triggered_at) >= T0 - 2 * 86400000) : rows;  // 3 days -> fewer rows
        return json({ triggers: rows2, total_count: total ?? rows2.length });
      }
      return json({});
    };
  },
});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '  ' + extra : ''}`); };
const sel = (id, v) => { $(id).value = v; $(id).dispatchEvent(new w.Event('change')); };
const type = v => { $('triggers-name-search').value = v; $('triggers-name-search').dispatchEvent(new w.Event('input')); };
const rows = () => [...d.querySelectorAll('#triggers-body tr')];
const vals = () => rows().map(r => r.querySelectorAll('td')[3] && r.querySelectorAll('td')[3].textContent).map(Number);   // metric_value = index i
const label = () => $('triggers-page-label').textContent, labelTop = () => $('triggers-page-label-top').textContent, count = () => $('triggers-count').textContent;
const resetShown = () => !$('btn-triggers-reset').classList.contains('hidden');
const noteShown = () => !$('triggers-limit-note').classList.contains('hidden');
const allVals = () => { sel('triggers-page-size', '25'); const out = []; for (let k = 0; k < 5; k++) { out.push(...vals()); if ($('btn-triggers-next').disabled) break; $('btn-triggers-next').click(); } sel('triggers-page-size', '10'); return out; };
const reset = () => $('btn-triggers-reset').click();
const expect = f => all.map((t, i) => [t, i]).filter(([t, i]) => f(t, i)).map(([, i]) => i);
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);

(async () => {
  await flush(); await flush();
  check('one request: since_days=30&limit=500&offset=0', reqs.length === 1 && reqs[0].endsWith('/alerts/triggers?since_days=30&limit=500&offset=0'), reqs[0]);
  check('no look-back control; subtitle fixed to 30 days', !$('triggers-lookback') && !d.querySelector('.triggers-filters').textContent.includes('Look back')
        && d.querySelector('#triggers-body').closest('.card').querySelector('.card-subtitle').textContent === 'Alert firings from the last 30 days, newest first');
  check('Show More is gone; Previous/Next above and below', !$('btn-show-more') && d.querySelectorAll('.triggers-prev').length === 2 && d.querySelectorAll('.triggers-next').length === 2);
  const order = [...d.querySelector('.triggers-filters').querySelectorAll('select, input')].map(e => e.id);
  check('filter row ids in order', same(order, ['triggers-from', 'triggers-to', 'triggers-name-search', 'triggers-cluster-filter', 'triggers-status-filter', 'triggers-recurrence-filter', 'triggers-teams-filter', 'triggers-page-size']), JSON.stringify(order));
  check('From/To are datetime-local; row wraps; Reset hidden; count above table', $('triggers-from').type === 'datetime-local' && $('triggers-to').type === 'datetime-local'
        && w.getComputedStyle(d.querySelector('.triggers-filters')).flexWrap === 'wrap' && !resetShown()
        && !!($('triggers-count').compareDocumentPosition($('triggers-body')) & w.Node.DOCUMENT_POSITION_FOLLOWING));
  check('40 firings, "40 firings, 6 open", newest first, both pagers', count() === '40 firings, 6 open' && same(vals(), [0,1,2,3,4,5,6,7,8,9]) && label() === 'Showing 1-10 of 40' && labelTop() === label(), count());
  check('cluster options from loaded rows, escaped', same([...$('triggers-cluster-filter').options].map(o => o.value + '=' + o.textContent), ['=All clusters', '4=c4', '8=c8', '9=c9 <i>']) && !$('triggers-cluster-filter').querySelector('i'));
  check('rows escape names and error detail', !d.querySelector('#triggers-body b') && rows()[0].querySelectorAll('td')[1].textContent === 'Broker CPU % - c4'
        && rows()[0].querySelector('.post-fail').title === 'HTTP 400 <bad>');
  check('columns kept: status, recurrence mark, post', rows()[0].querySelector('.status-open').textContent === '● Open' && rows()[0].querySelector('.recur-badge').textContent === '↻ Recurring'
        && rows()[1].querySelector('.status-resolved').textContent.startsWith('✓ Resolved ') && rows()[1].querySelector('.post-ok') && rows()[1].querySelectorAll('td').length === 7);
  const r0 = reqs.length;

  check('Teams Post options: All / Sent / Failed / Teams off', [...$('triggers-teams-filter').options].map(o => o.value + '=' + o.textContent).join() === '=All,sent=Sent,failed=Failed,off=Teams off');
  check('fixture: 7 failed, 4 Teams off', expect(t => !t.teams_post_success && !OFF.includes(t.error_detail)).length === 7 && same(expect(t => OFF.includes(t.error_detail)), [5, 15, 25, 35]));

  // each filter alone
  const cases = [
    ['cluster 8', () => sel('triggers-cluster-filter', '8'), (t) => t.cluster_id === 8],
    ['status Open', () => sel('triggers-status-filter', 'open'), (t) => !t.resolved_at],
    ['status Resolved', () => sel('triggers-status-filter', 'resolved'), (t) => !!t.resolved_at],
    ['recurrence Recurring', () => sel('triggers-recurrence-filter', 'recurring'), (t) => t.is_recurrence],
    ['recurrence New', () => sel('triggers-recurrence-filter', 'new'), (t) => !t.is_recurrence],
    ['teams Sent', () => sel('triggers-teams-filter', 'sent'), (t) => t.teams_post_success],
    ['teams Failed (Teams off excluded)', () => sel('triggers-teams-filter', 'failed'), (t) => !t.teams_post_success && !OFF.includes(t.error_detail)],
    ['teams Teams off', () => sel('triggers-teams-filter', 'off'), (t) => !t.teams_post_success && OFF.includes(t.error_detail)],
    ['name "  zOOkeeper  "', () => type('  zOOkeeper  '), (t) => t.config_name.toLowerCase().includes('zookeeper')],
    ['name "<b>max"', () => type('<b>max'), (t) => t.config_name.includes('<b>max')],
  ];
  for (const [lab, act, f] of cases) {
    act(); const got = allVals(), want = expect(f);
    check(`${lab} alone: ${want.length} firings, newest first, "Showing ${want.length} of 40 firings", Reset shown`, same(got, want) && count() === `Showing ${want.length} of 40 firings` && resetShown(), `${got.length} | ${count()}`);
    reset();
  }
  check('Reset hides itself; full list back', !resetShown() && count() === '40 firings, 6 open');

  // From / To bounds (UTC): i=4 at 02:00, i=8 at 20:00 the day before
  const at = i => all[i].triggered_at.slice(0, 16);
  $('triggers-from').value = at(8); $('triggers-from').dispatchEvent(new w.Event('change'));
  let got = allVals();
  check(`From ${at(8)} (inclusive) alone: i 0-8`, same(got, [0,1,2,3,4,5,6,7,8]) && count() === 'Showing 9 of 40 firings', at(8) + ' ' + JSON.stringify(got));
  reset();
  $('triggers-to').value = at(4); $('triggers-to').dispatchEvent(new w.Event('change'));
  got = allVals();
  check(`To ${at(4)} (whole minute inclusive) alone: i 4-39`, same(got, expect((t, i) => i >= 4)), at(4));
  $('triggers-from').value = at(8); $('triggers-from').dispatchEvent(new w.Event('change'));
  got = allVals();
  check('From + To: i 4-8', same(got, [4,5,6,7,8]) && label() === 'Showing 1-5 of 5', JSON.stringify(got));
  $('triggers-from').value = at(4); $('triggers-to').value = at(8); $('triggers-to').dispatchEvent(new w.Event('change'));
  check('From after To: nothing matches', rows().length === 1 && rows()[0].textContent.trim() === 'No firings match this filter.' && count() === 'Showing 0 of 40 firings', count());
  reset();
  // bound is UTC, not local: a To one minute before i=4 excludes it
  $('triggers-to').value = new Date(Date.parse(all[4].triggered_at) - 60000).toISOString().slice(0, 16); $('triggers-to').dispatchEvent(new w.Event('change'));
  check('To one minute earlier excludes that firing (UTC reading)', allVals()[0] === 5);
  reset();

  // all combined
  $('triggers-from').value = at(30); $('triggers-to').value = at(0); $('triggers-to').dispatchEvent(new w.Event('change'));
  sel('triggers-cluster-filter', '4'); sel('triggers-status-filter', 'resolved'); sel('triggers-recurrence-filter', 'new'); sel('triggers-teams-filter', 'sent'); type('urp');
  const want = expect((t, i) => i <= 30 && t.cluster_id === 4 && t.resolved_at && !t.is_recurrence && t.teams_post_success && t.config_name.startsWith('URP'));
  check(`all seven filters combined: ${JSON.stringify(want)}`, same(vals(), want) && count() === `Showing ${want.length} of 40 firings`, JSON.stringify(vals()));
  type('urp zzz');
  check('nothing matches: message row (colspan 7), "Showing 0 of 40 firings", pager "Showing 0 of 40", both disabled',
        rows().length === 1 && rows()[0].textContent.trim() === 'No firings match this filter.' && rows()[0].querySelector('td').colSpan === 7
        && count() === 'Showing 0 of 40 firings' && label() === 'Showing 0 of 40' && $('btn-triggers-prev').disabled && $('btn-triggers-next-top').disabled, label());
  check('no filter change made a request', reqs.length === r0, `${reqs.length - r0}`);

  // Reset keeps look-back and page size
  sel('triggers-page-size', '25');
  sel('triggers-cluster-filter', '4');
  reset();
  check('Reset: all seven filters cleared, per page 25 kept, page 1, no request',
        same(['triggers-from', 'triggers-to', 'triggers-name-search', 'triggers-cluster-filter', 'triggers-status-filter', 'triggers-recurrence-filter', 'triggers-teams-filter'].map(i => $(i).value), ['', '', '', '', '', '', ''])
        && $('triggers-page-size').value === '25' && label() === 'Showing 1-25 of 40' && reqs.length === r0, label());
  sel('triggers-page-size', '10');

  // paging
  $('btn-triggers-next-top').click();
  check('top Next -> page 2, both pagers in sync', label() === 'Showing 11-20 of 40' && labelTop() === label() && same(vals(), [10,11,12,13,14,15,16,17,18,19]) && !$('btn-triggers-prev').disabled);
  $('btn-triggers-next').click(); $('btn-triggers-next').click();
  check('last page: 31-40, Next disabled', label() === 'Showing 31-40 of 40' && $('btn-triggers-next').disabled && $('btn-triggers-next-top').disabled);
  $('btn-triggers-next').click();
  check('Next on last page stays', label() === 'Showing 31-40 of 40');
  for (const [lab, act] of [['cluster', () => sel('triggers-cluster-filter', '4')], ['status', () => sel('triggers-status-filter', 'resolved')], ['recurrence', () => sel('triggers-recurrence-filter', 'new')],
                            ['teams', () => sel('triggers-teams-filter', 'sent')], ['name', () => type('c')], ['From', () => { $('triggers-from').value = at(39); $('triggers-from').dispatchEvent(new w.Event('change')); }],
                            ['To', () => { $('triggers-to').value = at(0); $('triggers-to').dispatchEvent(new w.Event('change')); }], ['page size', () => sel('triggers-page-size', '10')],
                            ['× clear', () => $('btn-triggers-name-clear').click()], ['Reset', () => reset()]]) {
    if (lab === '× clear') type('c'); else if (lab !== 'Reset') reset(); else sel('triggers-status-filter', 'resolved');
    $('btn-triggers-next').click();
    const was = label(); act();
    check(`changing ${lab} returns to page 1`, was.startsWith('Showing 11-') && label().startsWith('Showing 1-'), `${was} -> ${label()}`);
  }
  type('zoo'); sel('triggers-status-filter', 'resolved'); $('btn-triggers-name-clear').click();
  check('× clears only the name', $('triggers-name-search').value === '' && $('triggers-status-filter').value === 'resolved' && resetShown());
  reset();

  // Refresh: reloads with the current look-back, keeps filters and page
  sel('triggers-status-filter', 'resolved'); $('btn-triggers-next').click();
  const before = [label(), count()];
  $('btn-refresh-triggers').click(); await flush();
  check('Refresh: one request with since_days=30&limit=500, filters and page 2 kept', reqs.at(-1).endsWith('?since_days=30&limit=500&offset=0') && same([label(), count()], before) && $('triggers-status-filter').value === 'resolved', `${before} -> ${label()}`);
  reset();

  // 500-row note
  check('note hidden when total_count <= rows', !noteShown());
  total = 812; $('btn-refresh-triggers').click(); await flush();
  check('total_count 812 > 40 fetched: note "Showing the newest 40 of 812 firings in this window"', noteShown() && $('triggers-limit-note').textContent === 'Showing the newest 40 of 812 firings in this window' && count() === '40 firings, 6 open');
  total = null;

  // cluster selection kept / dropped on reload
  sel('triggers-cluster-filter', '9'); $('btn-refresh-triggers').click(); await flush();
  check('cluster selection kept across a reload', $('triggers-cluster-filter').value === '9' && count() === 'Showing 13 of 40 firings', count());
  reset();

  // empty and error rows
  mode = 'empty'; $('btn-triggers-reset').click(); $('btn-refresh-triggers').click(); await flush();
  check('no firings: original empty row (colspan 7), pagers hidden, "0 firings, 0 open"', rows().length === 1 && rows()[0].textContent.trim() === 'No alerts have fired yet.' && rows()[0].querySelector('td').colSpan === 7
        && d.querySelectorAll('.triggers-pager.hidden').length === 2 && count() === '0 firings, 0 open' && !noteShown(), count());
  mode = 'error'; $('btn-refresh-triggers').click(); await flush();
  check('error: error row (colspan 7), pagers hidden, count cleared', rows().length === 1 && rows()[0].querySelector('.data-table__error') && rows()[0].textContent.includes('Failed to load recent triggers: ')
        && rows()[0].querySelector('td').colSpan === 7 && d.querySelectorAll('.triggers-pager.hidden').length === 2 && count() === '' && !noteShown(), rows()[0].textContent.trim());
  mode = 'ok'; $('btn-refresh-triggers').click(); await flush();
  check('recovers after the error', count().endsWith(' open') && label().startsWith('Showing 1-'));

  // rules table untouched
  check('rules table still renders its row and count', !!d.querySelector('#configs-body button.config-edit[data-id="20"]') && $('configs-count').textContent === '1 rule, 1 active', $('configs-count').textContent);

  console.log(fails ? `${fails} FAILED` : 'ALL PASSED');
  process.exit(fails ? 1 : 0);
})();
