const { TEAMS_HTML, ROUTES_ALERTS, groupMatchesAlertTypes, consecutive } = require('./_page');
// Lag rule form (step 2 of 3): the real teams.html in jsdom, fetch stubbed.
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');
const routesAlerts = fs.readFileSync(ROUTES_ALERTS, 'utf8');

const calls = [];
const clusters = [{ id: 4, name: 'c4', enabled: true }, { id: 8, name: 'c8', enabled: true }];
const GROUPS = {
  4: ['orders-svc', 'orders-batch', 'orders-replay', 'orders-audit', 'orders-dlq', 'orders-x', 'billing', '<b>x</b>', 'stale-one'],
  8: ['payments', 'refunds'],
};
const CONNECTORS = { 4: ['sink-orders', 'sink-billing', 'src-db'], 8: [] };
let groupsMode = 'ok';   // 'ok' | 'fail' | 'hang'
let hangRelease = null;
const base = { severity: 'warning', threshold: 1, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, enabled: true, email_enabled: false, open_trigger_count: 0, min_rf: null };
const configs = [
  { ...base, id: 50, name: 'CG lag - c4', alert_type: 'cluster.consumer_lag', cluster_id: 4, cluster_name: 'c4',
    config: { mode: 'simple', warning: 1000, critical: 10000, reminder_hours: 12,
              custom: [{ name: 'orders-svc', tiers: { warning: 50, critical: 500 } }, { name: '<b>x</b>', tiers: { info: 1, critical: 9 } }],
              blacklist: ['billing', 'gone-group', 'stale-one'] } },
  { ...base, id: 51, name: 'Conn lag - c8', alert_type: 'cluster.connector_lag', cluster_id: 8, cluster_name: 'c8',
    config: { mode: 'simple', warning: 200, custom: [], blacklist: [], reminder_hours: 6 } },
  { ...base, id: 20, name: 'Broker CPU % - c4', alert_type: 'broker.cpu_pct', cluster_id: 4, cluster_name: 'c4', config: { mode: 'simple', warning: 70, critical: 85 } },
];
let nextResponse = null;
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
      if (url.endsWith('/settings/clusters')) return json({ clusters });
      if (url.includes('/alerts/triggers')) return json({ triggers: [], total_count: 0 });
      if (url.endsWith('/alerts/configs') && method === 'GET') return json({ configs });
      calls.push({ method, url, body: opts.body ? JSON.parse(opts.body) : null });
      const m = url.match(/\/dashboard\/consumer-groups\?cluster_id=(\d+)$/);
      if (m) {
        if (groupsMode === 'fail') return json({ detail: 'boom' }, 500);
        if (groupsMode === 'hang') await new Promise(r => { hangRelease = r; });
        const g = GROUPS[m[1]] || [];
        return json(g.length ? { consumer_groups: g.map(group_id => ({ group_id, total_lag: 1 })), total: g.length } : { empty: true });
      }
      const k = url.match(/\/dashboard\/kafka-connect\?cluster_id=(\d+)$/);
      if (k) return json({ status: 'ok', connectors: (CONNECTORS[k[1]] || []).map(name => ({ name })) });
      if (nextResponse) { const r = nextResponse; nextResponse = null; return r; }
      return json({ config: { id: 100, cluster_name: 'c' + ((calls.at(-1).body || {}).cluster_id) } });
    };
  },
});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0, passes = 0;
const check = (label, cond, extra = '') => { cond ? passes++ : fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '  ' + extra : ''}`); };
const setType = v => { $('af-alert-type').value = v; $('af-alert-type').dispatchEvent(new w.Event('change')); };
const setCluster = v => { $('af-cluster').value = v; $('af-cluster').dispatchEvent(new w.Event('change')); };
const typeIn = (id, v) => { $(id).value = v; $(id).dispatchEvent(new w.Event('input')); };
const newForm = (name, type) => { $('btn-new-alert').click(); $('af-name').value = name; if (type) setType(type); };
async function save() { const n = calls.length; $('btn-save-alert').click(); await flush(); return calls.slice(n).filter(c => c.url.includes('/alerts/')); }
const msg = () => $('alert-form-msg').textContent;
const shown = id => !$(id).classList.contains('hidden');
const picks = () => [...$('af-lag-picks').querySelectorAll('button')].map(b => b.textContent);
const customRows = () => [...$('af-lag-custom-body').querySelectorAll('tr')].map(tr => {
  const ins = tr.querySelectorAll('input');
  return [tr.querySelector('td').textContent, ins[0].value, ins[1].value];
});
const namesCalls = () => calls.filter(c => c.url.includes('/dashboard/'));
const rowFor = id => [...d.querySelectorAll('#configs-body tr')].find(tr => tr.querySelector(`button.config-edit[data-id="${id}"]`));

(async () => {
  await flush();
  console.log('--- types ---');
  const opts = [...$('af-alert-type').querySelectorAll('optgroup[label="Cluster"] option')].map(o => o.value);
  check('both types under Cluster, right after the Schema Registry types; Cluster group = ALERT_TYPES', consecutive(opts,
    ['cluster.sr_nodes_down', 'cluster.sr_soft_deleted', 'cluster.consumer_lag', 'cluster.connector_lag']) && groupMatchesAlertTypes(w, 'Cluster'), JSON.stringify(opts));
  const label = v => $('af-alert-type').querySelector(`option[value="${v}"]`).textContent;
  const pyLabel = v => (routesAlerts.match(new RegExp(`"${v.replace('.', '\\.')}": "([^"]+)"`)) || [])[1];
  check('labels match _TYPE_LABELS', label('cluster.consumer_lag') === 'Consumer group lag' && label('cluster.connector_lag') === 'Connector lag'
    && pyLabel('cluster.consumer_lag') === 'Consumer group lag' && pyLabel('cluster.connector_lag') === 'Connector lag');

  console.log('--- lag fields shown only for the lag types (all types) ---');
  newForm('vis');
  const bad = [];
  for (const o of $('af-alert-type').options) {
    setType(o.value);
    const lag = o.value === 'cluster.consumer_lag' || o.value === 'cluster.connector_lag';
    if (shown('af-lag-field') !== lag) bad.push(o.value);
    if (lag && !shown('af-tiers-field')) bad.push(o.value + ':tiers');
  }
  check(`lag block only for the 2 lag types (${$('af-alert-type').options.length} types checked), tiers shown with it`, bad.length === 0, bad.join(','));
  setType('cluster.consumer_lag');
  check('unit "messages" on the tier inputs', [...d.querySelectorAll('.af-tier-unit')].every(e => e.textContent === ' (messages)'));
  check('reminder default 6, hints present', $('af-lag-reminder').value === '6'
    && html.includes('While a group stays above its tier, a reminder is sent at this interval.')
    && html.includes('Names must match exactly. A new or renamed group is covered by the generic thresholds.'));

  console.log('--- names per cluster ---');
  check('no cluster: asks for one, no request', $('af-lag-names-state').textContent === 'Select a cluster to list its consumer groups.' && !namesCalls().length, $('af-lag-names-state').textContent);
  groupsMode = 'hang';
  setCluster('4'); await flush();
  check('loading state while the request runs', $('af-lag-names-state').textContent === 'Loading names…', $('af-lag-names-state').textContent);
  groupsMode = 'ok'; hangRelease(); await flush();
  check('cluster 4 groups loaded via GET /dashboard/consumer-groups?cluster_id=4', namesCalls().at(-1).url === '/dashboard/consumer-groups?cluster_id=4'
    && $('af-lag-names-state').textContent === '9 consumer groups on this cluster.', $('af-lag-names-state').textContent);
  setCluster('8'); await flush();
  check('cluster change reloads (cluster 8)', namesCalls().at(-1).url === '/dashboard/consumer-groups?cluster_id=8' && $('af-lag-names-state').textContent === '2 consumer groups on this cluster.');
  setType('cluster.connector_lag'); setCluster('4'); await flush();
  check('connector names via GET /dashboard/kafka-connect?cluster_id=4', namesCalls().at(-1).url === '/dashboard/kafka-connect?cluster_id=4'
    && $('af-lag-names-state').textContent === '3 connectors on this cluster.' && $('af-lag-search').placeholder === 'Search connectors to add', $('af-lag-names-state').textContent);

  console.log('--- picker ---');
  newForm('pick', 'cluster.consumer_lag'); setCluster('4'); await flush();
  typeIn('af-tier-warning', '1000'); typeIn('af-tier-critical', '10000');
  typeIn('af-lag-search', 'ORDERS');
  check('case-insensitive, up to 5 shown, "6 matching"', JSON.stringify(picks()) === JSON.stringify(['orders-audit', 'orders-batch', 'orders-dlq', 'orders-replay', 'orders-svc'])
    && $('af-lag-search-count').textContent === '6 matching', JSON.stringify(picks()) + ' ' + $('af-lag-search-count').textContent);
  $('af-lag-picks').querySelectorAll('button')[4].click();
  check('click adds a row with the generic tiers as defaults; search cleared', JSON.stringify(customRows()) === JSON.stringify([['orders-svc', '1000', '10000']])
    && $('af-lag-search').value === '' && !shown('af-lag-custom-table') === false);
  typeIn('af-lag-search', 'orders');
  check('a name already in the table is not offered', !picks().includes('orders-svc') && picks().length === 5 && $('af-lag-search-count').textContent === '5 matching', JSON.stringify(picks()));
  typeIn('af-lag-blacklist', 'orders-batch');
  typeIn('af-lag-search', 'orders');
  check('a blacklisted name is not offered', !picks().includes('orders-batch') && $('af-lag-search-count').textContent === '4 matching', JSON.stringify(picks()));
  typeIn('af-lag-search', 'orders-svc');
  check('exact name already used: nothing offered, not even as typed', picks().length === 0, JSON.stringify(picks()));
  typeIn('af-lag-search', 'brand-new');
  check('unknown typed name offered as Add "…" (not found)', JSON.stringify(picks()) === JSON.stringify(['Add "brand-new" (not found)']), JSON.stringify(picks()));
  $('af-lag-picks').querySelector('button').click();
  check('typed name added', customRows().map(r => r[0]).join(',') === 'orders-svc,brand-new');
  const rm = $('af-lag-custom-body').querySelectorAll('button')[1]; rm.click();
  check('× removes a row', customRows().map(r => r[0]).join(',') === 'orders-svc' && $('af-lag-custom-count').textContent === '1 of at most 200');
  const warnInput = $('af-lag-custom-body').querySelector('input');
  warnInput.value = '75'; warnInput.dispatchEvent(new w.Event('input'));
  typeIn('af-lag-search', '<b');
  check('names escaped in the picker (<b>x</b>)', $('af-lag-picks').innerHTML.includes('&lt;b&gt;x&lt;/b&gt;') && !$('af-lag-picks').querySelector('b') && picks()[0] === '<b>x</b>', $('af-lag-picks').innerHTML);
  $('af-lag-picks').querySelector('button').click();
  check('names escaped in the table', $('af-lag-custom-body').innerHTML.includes('&lt;b&gt;x&lt;/b&gt;') && !$('af-lag-custom-body').querySelector('b') && customRows()[1][0] === '<b>x</b>');

  console.log('--- blacklist textarea ---');
  typeIn('af-lag-blacklist', '  billing  \n\n stale-one\nbilling\n   \n<i>nope</i>\nzzz\n');
  check('trim, blank lines ignored, duplicates dropped: "2 of 4 found on this cluster"', $('af-lag-blacklist-status').textContent === '2 of 4 found on this cluster', $('af-lag-blacklist-status').textContent);
  const unk = [...$('af-lag-blacklist-unknown').querySelectorAll('.af-lag-unknown')].map(e => e.textContent);
  check('unknown names marked "(not found)", escaped', JSON.stringify(unk) === JSON.stringify(['<i>nope</i> (not found)', 'zzz (not found)'])
    && !$('af-lag-blacklist-unknown').querySelector('i'), JSON.stringify(unk));
  typeIn('af-lag-search', 'bill');
  check('blacklisted billing not offered (only the typed text, as nothing matches)', JSON.stringify(picks()) === JSON.stringify(['Add "bill" (not found)'])
    && $('af-lag-search-count').textContent === '0 matching', JSON.stringify(picks()));

  console.log('--- save ---');
  typeIn('af-lag-reminder', '12');
  let sent = await save();
  const p = sent[0] && sent[0].body;
  check('POST carries generic tiers + custom + blacklist + reminder_hours', sent.length === 1 && sent[0].method === 'POST' && JSON.stringify(p.tiers) === '{"warning":1000,"critical":10000}'
    && JSON.stringify(p.custom) === JSON.stringify([{ name: 'orders-svc', tiers: { warning: 75, critical: 10000 } }, { name: '<b>x</b>', tiers: { warning: 1000, critical: 10000 } }])
    && JSON.stringify(p.blacklist) === JSON.stringify(['billing', 'stale-one', '<i>nope</i>', 'zzz']) && p.reminder_hours === 12 && p.cluster_id === 4 && p.alert_type === 'cluster.consumer_lag', JSON.stringify(p));

  newForm('overlap', 'cluster.consumer_lag'); setCluster('4'); await flush();
  typeIn('af-tier-warning', '100');
  typeIn('af-lag-search', 'orders-svc'); $('af-lag-picks').querySelector('button').click();
  $('af-lag-blacklist').value = 'orders-svc';   // pasted after the row was added
  sent = await save();
  check('a name in both lists blocks the save with a message naming it', sent.length === 0
    && msg() === '✗ "orders-svc" is in both Custom thresholds and Not alerted; remove it from one', msg());
  $('af-lag-blacklist').value = '';
  typeIn('af-lag-reminder', '0'); sent = await save();
  check('reminder outside 1-168 blocks the save', sent.length === 0 && msg() === '✗ Reminder every (hours) must be a whole number from 1 to 168', msg());
  typeIn('af-lag-reminder', '6');
  const w0 = $('af-lag-custom-body').querySelectorAll('input');
  w0[0].value = ''; w0[0].dispatchEvent(new w.Event('input')); w0[1].value = ''; w0[1].dispatchEvent(new w.Event('input'));
  sent = await save();
  check('a custom row without values blocks the save', sent.length === 0 && msg() === '✗ Custom threshold "orders-svc" needs a warning or critical value', msg());
  w0[0].value = '5'; w0[0].dispatchEvent(new w.Event('input'));
  $('af-lag-blacklist').value = Array.from({ length: 501 }, (_, i) => 'g' + i).join('\n');
  sent = await save();
  check('more than 500 blacklist names blocks the save', sent.length === 0 && msg() === '✗ Not alerted lists 501 names; at most 500', msg());
  $('af-lag-blacklist').value = '';
  nextResponse = json({ detail: "custom 'orders-svc': tiers.warning must be a whole number greater than 0" }, 422);
  sent = await save();
  check("API 422 text shown as today", sent.length === 1 && msg() === "✗ Save failed: custom 'orders-svc': tiers.warning must be a whole number greater than 0", msg());

  newForm('cpu', 'broker.cpu_pct'); setCluster('4'); typeIn('af-tier-warning', '70');
  sent = await save();
  check('other types never send custom/blacklist/reminder_hours', sent.length === 1 && !('custom' in sent[0].body) && !('blacklist' in sent[0].body) && !('reminder_hours' in sent[0].body), JSON.stringify(sent[0] && sent[0].body));

  console.log('--- limit 200 rows ---');
  newForm('max', 'cluster.consumer_lag'); setCluster('8'); await flush();
  for (let i = 0; i < 200; i++) { typeIn('af-lag-search', 'n' + i); $('af-lag-picks').querySelector('button').click(); }
  typeIn('af-lag-search', 'pay');
  check('at 200 rows the search is disabled and nothing is offered', customRows().length === 200 && $('af-lag-search').disabled && picks().length === 0
    && $('af-lag-search-count').textContent === 'Limit reached: at most 200 custom thresholds', $('af-lag-search-count').textContent);

  console.log('--- load failure still allows typing ---');
  groupsMode = 'fail';
  newForm('fail', 'cluster.consumer_lag'); setCluster('4'); await flush();
  check('"Could not load names" state', $('af-lag-names-state').textContent === 'Could not load names (boom). You can still type names.', $('af-lag-names-state').textContent);
  typeIn('af-lag-search', 'typed-group');
  check('typed name can be added (no "not found": names unknown)', JSON.stringify(picks()) === JSON.stringify(['Add "typed-group"']), JSON.stringify(picks()));
  $('af-lag-picks').querySelector('button').click();
  typeIn('af-lag-blacklist', 'a\nb');
  check('row added; blacklist says names could not be checked', customRows()[0][0] === 'typed-group' && $('af-lag-blacklist-status').textContent === '2 names (names could not be checked)', $('af-lag-blacklist-status').textContent);
  groupsMode = 'ok';

  console.log('--- table summary and edit ---');
  const cellText = id => rowFor(id).querySelectorAll('td')[4].textContent;
  check('table: generic tiers + "+2 custom, 3 not alerted"', cellText(50) === 'warn 1000 · crit 10000 messages +2 custom, 3 not alerted', cellText(50));
  check('table: no lists -> generic tiers only; other types unchanged', cellText(51) === 'warn 200 messages' && cellText(20) === 'warn 70 · crit 85 %', cellText(51) + ' | ' + cellText(20));
  check('type labels in the table', rowFor(50).querySelectorAll('td')[1].textContent.startsWith('Consumer group lag') && rowFor(51).querySelectorAll('td')[1].textContent.startsWith('Connector lag'));
  rowFor(50).querySelector('button.config-edit').click(); await flush();
  check('edit fills generic tiers, rows, blacklist, reminder', $('af-tier-warning').value === '1000' && $('af-tier-critical').value === '10000'
    && JSON.stringify(customRows()) === JSON.stringify([['orders-svc', '50', '500'], ['<b>x</b>', '', '9']])
    && $('af-lag-blacklist').value === 'billing\ngone-group\nstale-one' && $('af-lag-reminder').value === '12', JSON.stringify(customRows()));
  check('edit loads the rule cluster names; unknown blacklisted name marked', namesCalls().at(-1).url === '/dashboard/consumer-groups?cluster_id=4'
    && $('af-lag-blacklist-status').textContent === '2 of 3 found on this cluster' && $('af-lag-blacklist-unknown').textContent === 'gone-group (not found)', $('af-lag-blacklist-status').textContent);
  sent = await save();
  const u = sent[0] && sent[0].body;
  check('edit PUTs all parts (info tier of a custom row kept)', sent.length === 1 && sent[0].method === 'PUT' && sent[0].url.endsWith('/alerts/configs/50')
    && JSON.stringify(u.custom) === JSON.stringify([{ name: 'orders-svc', tiers: { warning: 50, critical: 500 } }, { name: '<b>x</b>', tiers: { info: 1, critical: 9 } }])
    && JSON.stringify(u.blacklist) === '["billing","gone-group","stale-one"]' && u.reminder_hours === 12 && !('alert_type' in u), JSON.stringify(u));
  rowFor(20).querySelector('button.config-edit').click(); await flush();
  check('editing another type: lag block hidden and empty', !shown('af-lag-field') && customRows().length === 0 && $('af-lag-blacklist').value === '');
  $('btn-new-alert').click(); setType('cluster.consumer_lag');
  check('new form resets the lag parts', customRows().length === 0 && $('af-lag-blacklist').value === '' && $('af-lag-reminder').value === '6');

  console.log('--- copy ---');
  calls.length = 0;
  rowFor(50).querySelector('button.config-copy').click(); await flush();
  check('lag rules get the Copy button (cluster rules)', !!rowFor(50).querySelector('button.config-copy') && !!rowFor(51).querySelector('button.config-copy'));

  console.log(`\n${passes}/${passes + fails} passed`);
  process.exit(fails ? 1 : 0);
})().catch(e => { console.error(e); process.exit(2); });
