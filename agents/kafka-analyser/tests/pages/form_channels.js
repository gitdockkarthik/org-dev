// Per-rule Teams and Email channels on the rules form, and how Recent Triggers
// shows a firing whose Teams card was deliberately not posted.
const { TEAMS_HTML } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');

const calls = [];
const clusters = [{ id: 4, name: 'c4', enabled: true }];
const rule = (id, extra) => ({ id, name: `CPU ${id}`, alert_type: 'broker.cpu_pct', cluster_id: 4, cluster_name: 'c4', severity: 'warning', threshold: 1,
  config: { mode: 'simple', warning: 70 }, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, open_trigger_count: 0, min_rf: null, ...extra });
const configs = [rule(20, { enabled: false, email_enabled: true }), rule(21, { enabled: true, email_enabled: false })];
const trig = (id, ok, detail) => ({ id, alert_config_id: 20, config_name: 'CPU 20', cluster_id: 4, cluster_name: 'c4',
  triggered_at: new Date(Date.parse('2026-10-15T08:00:00Z') - id * 60000).toISOString(), resolved_at: null,
  is_recurrence: false, metric_value: '90', teams_post_success: ok, error_detail: detail });
const triggers = [trig(1, false, 'Teams off for this rule'), trig(2, false, 'Teams switched off globally'), trig(3, false, 'HTTP 400'), trig(4, true, null)];
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });

const dom = new JSDOM(html, { runScripts: 'dangerously', url: 'http://localhost/', beforeParse(w) {
  w.Element.prototype.scrollIntoView = () => {};
  w.mountUserBadge = () => {}; w.fetchCurrentUser = async () => null;
  w.fetch = async (url, opts = {}) => {
    const method = opts.method || 'GET';
    if (url.endsWith('/settings/clusters')) return json({ clusters });
    if (url.includes('/alerts/triggers')) return json({ triggers, total_count: triggers.length });
    if (url.endsWith('/alerts/configs') && method === 'GET') return json({ configs });
    calls.push({ method, path: url.replace(/^.*?(\/alerts)/, '$1'), body: JSON.parse(opts.body) });
    return json({ config: { id: 100, cluster_id: 4, cluster_name: 'c4' } });
  };
}});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '\n    ' + extra : ''}`); };
const visible = id => !$(id).closest('.field').classList.contains('hidden');
const save = async () => { const n = calls.length; $('btn-save-alert').click(); await flush(); return calls.slice(n); };
const row = id => [...$('configs-body').querySelectorAll('tr')].find(r => r.querySelector(`.config-edit[data-id="${id}"]`));

(async () => {
  await flush();
  // Form fields
  $('btn-new-alert').click();
  check('create: "Teams" (ticked) and "Email" (unticked) checkboxes, both shown',
    d.querySelector('label[for="af-enabled"]').textContent === 'Teams' && d.querySelector('label[for="af-email"]').textContent === 'Email'
    && $('af-enabled').checked && !$('af-email').checked && visible('af-enabled') && visible('af-email'));
  check('Email hint says a rule with both off is not evaluated',
    $('af-email').closest('.field').querySelector('.hint').textContent === 'A rule with Teams and Email both off is not evaluated.');

  // Create, email only
  $('af-name').value = 'mail only'; $('af-tier-warning').value = '70';
  $('af-enabled').checked = false; $('af-email').checked = true;
  let s = await save();
  check('create email only: POST enabled false, email_enabled true',
    s.length === 1 && s[0].method === 'POST' && s[0].body.enabled === false && s[0].body.email_enabled === true, JSON.stringify(s.map(c => c.body)));

  // Create, defaults
  $('btn-new-alert').click();
  check('form reset after save: Teams ticked, Email unticked', $('af-enabled').checked && !$('af-email').checked);
  $('af-name').value = 'defaults'; $('af-tier-warning').value = '70';
  s = await save();
  check('create defaults: enabled true, email_enabled false', s.length === 1 && s[0].body.enabled === true && s[0].body.email_enabled === false, JSON.stringify(s.map(c => c.body)));

  // Edit: Email filled from the rule and sent; Teams stays with the table toggle
  row(20).querySelector('.config-edit').click();
  check('edit email-only rule: Email ticked and shown, Teams field hidden', $('af-email').checked && visible('af-email') && !visible('af-enabled'));
  $('af-email').checked = false;
  s = await save();
  check('edit: PUT sends email_enabled false and no enabled', s.length === 1 && s[0].method === 'PUT' && s[0].path === '/alerts/configs/20'
    && s[0].body.email_enabled === false && !('enabled' in s[0].body), JSON.stringify(s.map(c => c.body)));
  row(21).querySelector('.config-edit').click();
  check('edit Teams-only rule: Email unticked', !$('af-email').checked);
  $('btn-cancel-alert').click();

  // Recent Triggers: Teams off is not shown as a failure
  const rows = [...$('triggers-body').querySelectorAll('tr')];
  const post = i => rows[i].querySelectorAll('td')[6];
  check('"Teams off for this rule": text "Teams off", reason in tooltip, no red mark',
    post(0).textContent.trim() === 'Teams off' && post(0).querySelector('span').title === 'Teams off for this rule' && !post(0).querySelector('.post-fail'), post(0).innerHTML);
  check('"Teams switched off globally": text "Teams off", reason in tooltip',
    post(1).textContent.trim() === 'Teams off' && post(1).querySelector('span').title === 'Teams switched off globally' && !post(1).querySelector('.post-fail'), post(1).innerHTML);
  check('other failures unchanged: red mark with the error', post(2).querySelector('.post-fail') && post(2).querySelector('.post-fail').title === 'HTTP 400');
  check('posted unchanged: green tick', post(3).querySelector('.post-ok') && post(3).textContent.trim() === '✓');

  // Teams Post filter: Teams off is its own option, never "Failed"
  const teamsFilter = v => { $('triggers-teams-filter').value = v; $('triggers-teams-filter').dispatchEvent(new w.Event('change'));
    return [...$('triggers-body').querySelectorAll('tr')].map(r => r.querySelectorAll('td')[6].textContent.trim()).join('|'); };
  check('filter Failed: only the HTTP 400 firing', teamsFilter('failed') === '✗' && $('triggers-count').textContent === 'Showing 1 of 4 firings', $('triggers-count').textContent);
  check('filter Teams off: both Teams-off firings', teamsFilter('off') === 'Teams off|Teams off' && $('triggers-count').textContent === 'Showing 2 of 4 firings', $('triggers-count').textContent);
  check('filter Sent: only the posted firing', teamsFilter('sent') === '✓');
  teamsFilter('');

  // Rules count: the email-only rule 20 counts as active
  check('count "2 rules, 2 active" (email-only rule 20 and Teams-only rule 21)', $('configs-count').textContent === '2 rules, 2 active', $('configs-count').textContent);
  console.log(fails ? `${fails} FAILED` : 'ALL PASS');
})();
