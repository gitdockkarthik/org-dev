const { TEAMS_HTML } = require('./_page');
// Delivery log card (teams.html): the real page in jsdom, fetch stubbed with a
// small fake of GET /alerts/notifications, /{id} and /summary.
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');

const EVIL = '<img src=x onerror="window.__pwned=1">';
const clusters = [{ id: 4, name: 'c4', enabled: true }, { id: 8, name: 'c8', enabled: true }, { id: 9, name: 'old ' + EVIL, enabled: false }];
const item = (id, over = {}) => ({
  id, created_at: new Date(Date.UTC(2026, 9, 12, 8, 0) - id * 60000).toISOString(), kind: 'rule_fire', source: 'teams_alerts',
  origin: 'inline', routing: 'rule', alert_config_id: 1, rule_name: 'CPU rule', cluster_id: 4, cluster_name: 'c4',
  severity: 'warning', title: `Notice ${id}`, state: 'delivered', delivered_at: null,
  email_status: 'sent', email_attempts: 1, email_last_error: null, email_sent_at: null,
  teams_status: 'sent', teams_attempts: 1, teams_last_error: null, teams_sent_at: null, ...over,
});
const SPECIAL = [
  item(1, { origin: 'outbox', kind: 'owner_summary', rule_name: null, source: 'hourly_summary', cluster_id: null, cluster_name: null,
            teams_status: 'superseded', teams_attempts: 0, email_attempts: 2 }),
  item(2, { email_status: 'failed', email_last_error: 'SMTP 535 authentication failed', state: 'degraded' }),
  item(3, { email_status: 'skipped', email_attempts: 0, email_last_error: 'rule email off', teams_status: 'abandoned',
            teams_last_error: 'gave up after 6 attempts', state: 'failed' }),
  item(4, { state: 'pending', email_status: 'pending', email_attempts: 0, teams_status: 'standby', teams_attempts: 0 }),
];
let dataset = [...SPECIAL, ...Array.from({ length: 19 }, (_, i) => item(5 + i))];  // 23 rows
let listMode = 'ok';  // 'ok' | 'fail'
let summary;
const SUMMARY_FULL = {
  window_hours: 24, total: 15, by_state: { pending: 1, delivered: 12, degraded: 1, failed: 1, suppressed: 0 },
  by_channel_status: {}, oldest_unfinished: { id: 4, kind: 'owner_watchdog', created_at: '2026-10-12T05:00:00+00:00', age_seconds: 3 * 3600 + 25 * 60 },
  delivery_job: { job_id: 'kafka-notification-delivery', last_success_started_at: '2026-10-12T08:03:00+00:00', last_success_ended_at: '2026-10-12T08:03:01+00:00' },
  last_heartbeat: { id: 7, created_at: '2026-10-12T06:00:00+00:00', state: 'delivered', email_status: 'skipped', teams_status: 'sent' },
};
const SUMMARY_NULLS = { window_hours: 24, total: 0, by_state: { pending: 0, delivered: 0, degraded: 0, failed: 0, suppressed: 0 },
  oldest_unfinished: null, delivery_job: { job_id: 'kafka-notification-delivery', last_success_started_at: null, last_success_ended_at: null },
  last_heartbeat: null };
summary = SUMMARY_FULL;
const DETAIL = {
  ...SPECIAL[1], attempts: [
    { channel: 'email', attempt_no: 1, started_at: '2026-10-12T07:58:01.500000+00:00', duration_ms: 30000, phase_reached: 'auth', outcome: 'failed_config', error: 'SMTP 535 authentication failed' },
    { channel: 'teams', attempt_no: 1, started_at: '2026-10-12T07:58:02+00:00', duration_ms: 420, phase_reached: 'done', outcome: 'sent', error: null },
    { channel: 'email', attempt_no: 0, started_at: null, duration_ms: null, phase_reached: null, outcome: null, error: null },
  ],
};
let detailMode = 'ok';
const reqs = [];  // every /alerts/notifications* URL
let triggerReqs = 0;
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });

const dom = new JSDOM(html, {
  runScripts: 'dangerously',
  url: 'http://localhost/',
  beforeParse(w) {
    w.Element.prototype.scrollIntoView = () => {};
    w.mountUserBadge = () => {};
    w.fetchCurrentUser = async () => null;
    w.fetch = async (url) => {
      if (url.endsWith('/settings/clusters')) return json({ clusters });
      if (url.includes('/alerts/triggers')) { triggerReqs++; return json({ triggers: [], total_count: 0 }); }
      if (url.endsWith('/alerts/configs')) return json({ configs: [] });
      if (url.includes('/alerts/notifications')) {
        reqs.push(url);
        const u = new URL(url, 'http://x');
        if (u.pathname.endsWith('/summary')) return json(summary);
        const m = u.pathname.match(/\/alerts\/notifications\/(\d+)$/);
        if (m) {
          if (detailMode === 'fail') return json({ detail: 'Notification 2 not found' }, 404);
          return json(m[1] === '2' ? DETAIL : { ...dataset.find(r => String(r.id) === m[1]), attempts: [] });
        }
        if (listMode === 'fail') return json({ detail: 'boom ' + EVIL }, 500);
        const limit = Number(u.searchParams.get('limit')), offset = Number(u.searchParams.get('offset'));
        return json({ notifications: dataset.slice(offset, offset + limit), total_count: dataset.length, limit, offset });
      }
      return json({});
    };
  },
});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0, passes = 0;
const check = (label, cond, extra = '') => { cond ? passes++ : fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '  ' + extra : ''}`); };
const listReqs = () => reqs.filter(u => /\/alerts\/notifications\?/.test(u));
const lastParams = () => Object.fromEntries(new URL(listReqs().at(-1), 'http://x').searchParams);
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const sel = async (id, v) => { $(id).value = v; $(id).dispatchEvent(new w.Event('change')); await flush(); };
const rows = () => [...$('dl-body').querySelectorAll('tr.dl-row')];
const rowById = id => rows().find(tr => tr.dataset.id === String(id));
const cells = tr => [...tr.querySelectorAll('td')];
const labels = () => [...d.querySelectorAll('.dl-page-label')].map(e => e.textContent);
const shown = id => !$(id).classList.contains('hidden');
const opts = id => [...$(id).options].map(o => `${o.value}=${o.textContent}`);
const click = async el => { el.dispatchEvent(new w.MouseEvent('click', { bubbles: true })); await flush(); };

(async () => {
  await flush();
  console.log('--- card and filters ---');
  const card = $('dl-body').closest('.card');
  check('card title and subtitle', card.querySelector('.card-title').textContent === 'Delivery log'
    && card.querySelector('.card-subtitle').textContent === 'Every notification the agent sent: its channels, status and any error. Last 24 hours by default, up to 7 days.');
  check('card comes after Recent Triggers, inside the alerts panel', $('triggers-body').closest('.card').nextElementSibling === card && card.closest('#panel-alerts'));
  check('Period: 24 hours (default), 3 days, 7 days', same(opts('dl-period'), ['24=24 hours', '72=3 days', '168=7 days']) && $('dl-period').value === '24');
  check('Kind options', same(opts('dl-kind'), ['=All', 'owner_summary=owner summary', 'owner_watchdog=owner watchdog', 'rule_fire=rule fire',
    'rule_escalate=rule escalate', 'rule_reminder=rule reminder', 'rule_resolve=rule resolve', 'heartbeat=heartbeat', 'deadman=deadman']));
  check('Status options', same(opts('dl-state'), ['=All', 'pending=pending', 'delivered=delivered', 'degraded=degraded', 'failed=failed']));
  check('Channel options', same(opts('dl-channel'), ['=All', 'email=Email', 'teams=Teams']));
  check('Channel status options (standby included)', same(opts('dl-channel-status'), ['=All', 'sent=sent', 'failed=failed', 'skipped=skipped', 'pending=pending', 'standby=standby', 'abandoned=abandoned']));
  check('Cluster options from /settings/clusters (disabled too), names escaped', same([...$('dl-cluster').options].map(o => o.value), ['', '4', '8', '9'])
    && $('dl-cluster').options[3].textContent === 'old ' + EVIL && !$('dl-cluster').querySelector('img'));
  check('Per page 10 (default) or 25; Reset hidden', same(opts('dl-page-size'), ['10=10', '25=25']) && $('dl-page-size').value === '10' && !shown('btn-dl-reset'));
  check('filter row wraps (flex-wrap) like Recent Triggers', w.getComputedStyle(d.querySelector('.dl-filters')).flexWrap === 'wrap');
  const heads = [...card.querySelectorAll('thead')[0].querySelectorAll('th')].map(th => th.textContent);
  check('8 columns', same(heads, ['Time (UTC)', 'Kind', 'Rule or source', 'Cluster', 'Title', 'Email', 'Teams', 'State']), JSON.stringify(heads));

  console.log('--- initial load ---');
  check('one list request: since_hours=24, limit=10, offset=0 (no 500-row load)', listReqs().length === 1 && same(lastParams(), { since_hours: '24', limit: '10', offset: '0' }), listReqs()[0]);
  check('one summary request', reqs.filter(u => u.endsWith('/alerts/notifications/summary')).length === 1);
  check('Recent Triggers still one request', triggerReqs === 1);
  check('count line and pagers', $('dl-count').textContent === '23 notifications in the last 24 hours' && same(labels(), ['Showing 1-10 of 23', 'Showing 1-10 of 23'])
    && $('btn-dl-prev').disabled && $('btn-dl-prev-top').disabled && !$('btn-dl-next').disabled, $('dl-count').textContent);
  check('10 rows, newest first as served', rows().length === 10 && rows()[0].dataset.id === '1');

  console.log('--- summary ---');
  check('summary line (full)', $('dl-summary').textContent === 'Last 24 hours: 12 delivered, 1 degraded, 1 failed, 1 pending · oldest unfinished 3 h 25 min old'
    + ' · delivery worker last ran OK 2026-10-12 08:03 UTC · last heartbeat 2026-10-12 06:00 UTC (delivered)', $('dl-summary').textContent);

  console.log('--- row content and styling ---');
  const r1 = cells(rowById(1)), r2 = cells(rowById(2)), r3 = cells(rowById(3)), r4 = cells(rowById(4)), r5 = cells(rowById(5));
  check('origin: outbox shown as "worker", inline as "inline"', r1[0].querySelector('.dl-sub').textContent === 'worker' && r5[0].querySelector('.dl-sub').textContent === 'inline');
  check('time, kind text, source when no rule, cluster "—" for owner notices', r1[0].textContent.startsWith('2026-10-12 07:59 UTC') && r1[1].textContent === 'owner summary'
    && r1[2].textContent === 'hourly_summary' && r1[3].textContent === '—' && r5[2].textContent === 'CPU rule' && r5[3].textContent === 'c4');
  check('delivered: ✓ sent on both channels, attempts, green state', r5[5].querySelector('.dl-ch-ok').textContent === '✓ sent' && r5[5].querySelector('.dl-sub').textContent === '1 attempt'
    && r5[7].querySelector('.dl-state--delivered').textContent === 'delivered' && r1[5].querySelector('.dl-sub').textContent === '2 attempts');
  const f2 = r2[5].querySelector('.dl-ch-fail');
  check('failed email: red mark, last error in tooltip; degraded state amber', f2 && f2.textContent === '✗ failed' && f2.title === 'SMTP 535 authentication failed'
    && w.getComputedStyle(f2).color === 'rgb(220, 38, 38)' && r2[7].querySelector('.dl-state--degraded')
    && w.getComputedStyle(r2[7].querySelector('.dl-state')).color === 'rgb(217, 119, 6)');
  const f3 = r3[6].querySelector('.dl-ch-fail');
  check('abandoned Teams: red mark with its error; skipped email shows its reason; failed state red', f3 && f3.textContent === '✗ abandoned' && f3.title === 'gave up after 6 attempts'
    && r3[5].querySelector('.dl-ch-skip').title === 'rule email off' && r3[7].querySelector('.dl-state--failed'));
  check('pending plain, standby as a neutral "standby (fallback)" with a tooltip, pending state grey', r4[5].querySelector('.dl-ch-wait').textContent === 'pending'
    && r4[6].querySelector('.dl-ch-wait').textContent === 'standby (fallback)' && r4[6].querySelector('.dl-ch-wait').title === 'Sent to Teams only if the email cannot be delivered'
    && !r4[6].querySelector('.dl-ch-fail, .dl-ch-skip') && r4[7].querySelector('.dl-state--pending') && !r4[5].querySelector('.dl-ch-fail') && r1[6].querySelector('.dl-ch-wait').textContent === 'superseded');

  console.log('--- paging ---');
  await click($('btn-dl-next'));
  check('Next: offset 10, labels 11-20, both pagers in sync', same(lastParams(), { since_hours: '24', limit: '10', offset: '10' }) && same(labels(), ['Showing 11-20 of 23', 'Showing 11-20 of 23'])
    && !$('btn-dl-prev-top').disabled);
  await click($('btn-dl-next-top'));
  check('top Next: offset 20, last page 21-23, Next disabled', lastParams().offset === '20' && labels()[0] === 'Showing 21-23 of 23' && rows().length === 3
    && $('btn-dl-next').disabled && $('btn-dl-next-top').disabled);
  await click($('btn-dl-prev'));
  check('Previous: offset 10', lastParams().offset === '10' && labels()[0] === 'Showing 11-20 of 23');
  await sel('dl-page-size', '25');
  check('page size 25: limit 25, offset 0, all 23 rows, pagers disabled', same(lastParams(), { since_hours: '24', limit: '25', offset: '0' }) && rows().length === 23
    && $('btn-dl-prev').disabled && $('btn-dl-next').disabled);
  await sel('dl-page-size', '10');

  console.log('--- filters: one request each, back to page 1 ---');
  const filterCases = [
    ['dl-period', '72', { since_hours: '72' }], ['dl-period', '168', { since_hours: '168' }], ['dl-kind', 'rule_resolve', { kind: 'rule_resolve' }],
    ['dl-state', 'degraded', { state: 'degraded' }], ['dl-channel', 'teams', { channel: 'teams' }],
    ['dl-channel-status', 'abandoned', { channel_status: 'abandoned' }], ['dl-channel-status', 'standby', { channel_status: 'standby' }],
    ['dl-cluster', '8', { cluster_id: '8' }],
  ];
  for (const [id, value, want] of filterCases) {
    await click($('btn-dl-next'));
    const n = listReqs().length;
    await sel(id, value);
    const p = lastParams();
    check(`${id}=${value}: one request, ${JSON.stringify(want)}, offset 0, Reset shown`, listReqs().length === n + 1 && same(p, { since_hours: '24', ...want, limit: '10', offset: '0' })
      && labels()[0] === 'Showing 1-10 of 23' && shown('btn-dl-reset'), JSON.stringify(p));
    await sel(id, id === 'dl-period' ? '24' : '');
  }
  check('back to defaults: Reset hidden', !shown('btn-dl-reset'));
  await sel('dl-period', '168'); await sel('dl-kind', 'rule_fire'); await sel('dl-state', 'failed'); await sel('dl-channel', 'email');
  await sel('dl-channel-status', 'failed'); await sel('dl-cluster', '4');
  check('combined filters in one URL', same(lastParams(), { since_hours: '168', kind: 'rule_fire', state: 'failed', channel: 'email', channel_status: 'failed', cluster_id: '4', limit: '10', offset: '0' }),
    JSON.stringify(lastParams()));
  check('count line with filters', $('dl-count').textContent === '23 matching notifications in the last 7 days', $('dl-count').textContent);
  await sel('dl-page-size', '25'); await click($('btn-dl-next'));
  const n0 = listReqs().length;
  await click($('btn-dl-reset'));
  check('Reset: one request, every filter cleared, 24 hours, per page 25 kept, page 1, Reset hidden', listReqs().length === n0 + 1
    && same(lastParams(), { since_hours: '24', limit: '25', offset: '0' }) && ['dl-kind', 'dl-state', 'dl-channel', 'dl-channel-status', 'dl-cluster'].every(id => $(id).value === '')
    && $('dl-period').value === '24' && $('dl-page-size').value === '25' && !shown('btn-dl-reset'), JSON.stringify(lastParams()));
  await sel('dl-page-size', '10');

  console.log('--- detail panel ---');
  check('detail hidden at first', !shown('dl-detail'));
  await click(cells(rowById(2))[4]);
  check('row click: GET /alerts/notifications/2, panel shown, row marked', reqs.at(-1).endsWith('/alerts/notifications/2') && shown('dl-detail') && rowById(2).classList.contains('dl-selected'));
  const at = [...$('dl-detail-body').querySelectorAll('tr')].map(tr => cells(tr).map(td => td.textContent));
  check('attempts: channel, attempt, time, duration, phase, outcome, error', same(at, [
    ['Email', '1', '2026-10-12 07:58:01', '30.0 s', 'auth', 'failed_config', 'SMTP 535 authentication failed'],
    ['Teams', '1', '2026-10-12 07:58:02', '420 ms', 'done', 'sent', ''],
    ['Email', 'not attempted', '—', '—', '—', 'in flight', ''],
  ]), JSON.stringify(at));
  check('detail title and meta', $('dl-detail-title').textContent === 'Notification 2: rule fire · Notice 2' && $('dl-detail-meta').textContent.includes('CPU rule · c4 · state degraded'), $('dl-detail-meta').textContent);
  const detailCard = $('dl-detail').closest('.card');
  check('panel sits below the table in the same card', detailCard === card && $('dl-pager').compareDocumentPosition($('dl-detail')) & w.Node.DOCUMENT_POSITION_FOLLOWING);
  rowById(5).dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Enter', bubbles: true })); await flush();
  check('Enter on a row opens it; no attempts -> message row', reqs.at(-1).endsWith('/alerts/notifications/5') && $('dl-detail-body').textContent.includes('No attempts recorded.')
    && $('dl-detail-body').querySelector('td').colSpan === 7);
  await click($('btn-dl-detail-close'));
  check('Close hides the panel and clears the mark', !shown('dl-detail') && !d.querySelector('#dl-body .dl-selected'));
  detailMode = 'fail';
  await click(cells(rowById(2))[0]);
  check('detail error shown as text', $('dl-detail-meta').textContent === 'Failed to load notification 2: Notification 2 not found', $('dl-detail-meta').textContent);
  detailMode = 'ok';
  await click($('btn-dl-detail-close'));

  console.log('--- refresh, null summary ---');
  summary = SUMMARY_NULLS;
  const before = reqs.length;
  await click($('btn-refresh-dl'));
  check('Refresh: one list and one summary request, same page', reqs.length === before + 2 && reqs.slice(before).some(u => u.endsWith('/summary')) && lastParams().offset === '0');
  check('summary line with nulls', $('dl-summary').textContent === 'Last 24 hours: no notifications · nothing waiting · delivery worker: no successful run found · heartbeat: not enabled yet',
    $('dl-summary').textContent);
  check('Refresh did not reload Recent Triggers', triggerReqs === 1);

  console.log('--- empty, error, aged-out ---');
  const all = dataset;
  dataset = [];
  await click($('btn-refresh-dl'));
  check('empty: message row colspan 8, pagers hidden', $('dl-body').textContent.trim() === 'No notifications in this period.' && $('dl-body').querySelector('td').colSpan === 8
    && !shown('dl-pager') && !shown('dl-pager-top') && $('dl-count').textContent === '0 notifications in the last 24 hours');
  await sel('dl-kind', 'deadman');
  check('empty with a filter: its own message', $('dl-body').textContent.trim() === 'No notifications match these filters.');
  await sel('dl-kind', '');
  dataset = all;
  listMode = 'fail';
  await click($('btn-refresh-dl'));
  const errTd = $('dl-body').querySelector('td');
  check('error row: colspan 8, message escaped, pagers hidden, count cleared', errTd.className === 'data-table__error' && errTd.colSpan === 8
    && errTd.textContent === 'Failed to load the delivery log: boom ' + EVIL && !errTd.querySelector('img') && !shown('dl-pager') && $('dl-count').textContent === '');
  listMode = 'ok';
  await click($('btn-refresh-dl'));
  await click($('btn-dl-next')); await click($('btn-dl-next'));
  dataset = all.slice(0, 15);
  const n1 = listReqs().length;
  await click($('btn-refresh-dl'));
  check('rows aged out: page 3 is empty now, so the last page (11-15) is loaded', listReqs().length === n1 + 2 && lastParams().offset === '10' && labels()[0] === 'Showing 11-15 of 15');
  dataset = all;

  console.log('--- escaping ---');
  dataset = [item(66, { kind: EVIL, rule_name: EVIL, cluster_name: EVIL, title: EVIL, state: EVIL, origin: EVIL, email_status: 'failed',
    email_last_error: '" onmouseover="window.__pwned=2', teams_status: EVIL, id: 66 })];
  await click($('btn-refresh-dl'));
  const tr = rowById(66);
  check('hostile values render as text: no element injected, no handler ran', tr && !$('dl-body').querySelector('img') && w.__pwned === undefined
    && cells(tr)[2].textContent === EVIL && cells(tr)[4].textContent === EVIL && cells(tr)[0].querySelector('.dl-sub').textContent === EVIL);
  const tip = cells(tr)[5].querySelector('.dl-ch-fail');
  check('hostile error stays inside the tooltip; unknown state gets the neutral class', tip.title === '" onmouseover="window.__pwned=2' && !tip.hasAttribute('onmouseover')
    && cells(tr)[7].querySelector('.dl-state').className === 'dl-state dl-state--pending');
  summary = { by_state: { delivered: 1 }, oldest_unfinished: null, delivery_job: {}, last_heartbeat: { created_at: '2026-10-12T06:00:00Z', state: EVIL } };
  await click($('btn-refresh-dl'));
  check('summary uses textContent', $('dl-summary').textContent.endsWith(`(${EVIL})`) && !$('dl-summary').querySelector('img'));
  w.eval("openDlDetail('1 OR 1=1')"); await flush();
  check('detail ignores a non-numeric id (no request)', !reqs.at(-1).includes('OR'));

  console.log(`\n${passes} passed, ${fails} failed`);
  process.exit(fails ? 1 : 0);
})().catch(e => { console.error(e); process.exit(2); });
