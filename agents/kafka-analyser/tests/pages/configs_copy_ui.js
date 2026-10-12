const { TEAMS_HTML } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');

const calls = []; let configGets = 0; let copyReply = null;
const clusters = [{ id: 4, name: 'c4', enabled: true }, { id: 8, name: 'c8', enabled: true }, { id: 9, name: '<img src=x onerror=alert(1)>', enabled: true }];
const cfg = (id, name, alert_type, cluster_id, cluster_name) => ({ id, name, alert_type, cluster_id, cluster_name, severity: 'warning', threshold: 1,
  config: alert_type.startsWith('broker.') ? { mode: 'simple', warning: 70 } : { threshold: 1 }, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, enabled: true, email_enabled: false, open_trigger_count: 0 });
const configs = [cfg(3, 'Broker unreachable', 'broker_unreachable', null, 'All clusters'), cfg(5, 'CLOSE_WAIT spike', 'close_wait_spike', null, 'All clusters'),
  cfg(20, 'Broker CPU % - c4', 'broker.cpu_pct', 4, 'c4'), cfg(21, 'Broker heap % - c4 <script>', 'broker.heap_pct', 4, 'c4')];
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });

const dom = new JSDOM(html, { runScripts: 'dangerously', url: 'http://localhost/', beforeParse(w) {
  w.Element.prototype.scrollIntoView = () => {};
  w.mountUserBadge = () => {}; w.fetchCurrentUser = async () => null;
  w.fetch = async (url, opts = {}) => {
    const method = opts.method || 'GET';
    if (url.endsWith('/settings/clusters')) return json({ clusters });
    if (url.includes('/alerts/triggers')) return json({ triggers: [], total_count: 0 });
    if (url.endsWith('/alerts/configs') && method === 'GET') { configGets++; return json({ configs }); }
    calls.push({ method, url, body: opts.body ? JSON.parse(opts.body) : null });
    // Buttons must be disabled while the call is in flight.
    calls[calls.length - 1].buttonsDisabled = ['btn-copy-preview', 'btn-copy-run', 'btn-copy-cancel'].every(id => w.document.getElementById(id).disabled);
    return typeof copyReply === 'function' ? copyReply(JSON.parse(opts.body)) : json(copyReply);
  };
}});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '\n    ' + extra : ''}`); };
const row = id => d.querySelector(`.config-edit[data-id="${id}"]`).closest('tr');
const copyBtn = id => d.querySelector(`.config-copy[data-id="${id}"]`);
const targets = () => [...$('copy-clusters').querySelectorAll('.copy-target')];
const tick = (...ids) => targets().forEach(b => { b.checked = ids.includes(Number(b.value)); });

(async () => {
  await flush();
  check('Copy button on CPU row (20), before Edit, cell nowrap', !!copyBtn(20) && copyBtn(20).nextElementSibling.classList.contains('config-edit') && copyBtn(20).closest('td').classList.contains('nowrap'), row(20).lastElementChild.innerHTML.trim());
  check('Copy button on heap row (21)', !!copyBtn(21) && copyBtn(21).className === 'btn btn-secondary btn-sm config-copy');
  check('no Copy button on reachability (3) or CLOSE_WAIT (5)', !copyBtn(3) && !copyBtn(5), row(5).lastElementChild.innerHTML.trim());
  check('panel hidden initially', $('copy-panel').classList.contains('hidden'));

  copyBtn(20).click();
  const labels = targets().map(b => b.value + '=' + d.querySelector(`label[for="${b.id}"]`).textContent);
  check('panel shown; lists other clusters, not source 4; all unchecked', !$('copy-panel').classList.contains('hidden') && targets().map(b => b.value).join() === '8,9' && targets().every(b => !b.checked), `${$('copy-source').textContent} | ${labels.join(' | ')}`);
  check('cluster name escaped in checkbox label (no <img> element)', !$('copy-clusters').querySelector('img'), $('copy-clusters').querySelector('label[for="copy-target-9"]').innerHTML);
  check('note present', $('copy-panel').textContent.includes('Copies are created disabled; a cluster that already has this rule is skipped'));

  let n = calls.length;
  $('btn-copy-preview').click(); await flush();
  check('nothing ticked -> message, no request', calls.length === n && $('copy-msg').textContent === '✗ Tick at least one cluster' && $('copy-msg').classList.contains('err'), `msg='${$('copy-msg').textContent}'`);

  tick(8, 9);
  copyReply = { preview: true, created: [{ id: null, alert_type: 'broker.cpu_pct', cluster_id: 9, cluster_name: '<img src=x onerror=alert(1)>', name: 'Broker CPU % - <img>' }],
                skipped: [{ alert_type: 'broker.cpu_pct', cluster_id: 8, cluster_name: 'c8', reason: 'exists (id 100) <b>' }] };
  const g0 = configGets;
  $('btn-copy-preview').click(); await flush();
  let c = calls[calls.length - 1];
  check('Preview sends config_id 20, targets [8,9], preview true; buttons disabled during call', c.url.endsWith('/alerts/configs/copy') && c.method === 'POST' && JSON.stringify(c.body) === '{"config_id":20,"target_cluster_ids":[8,9],"preview":true}' && c.buttonsDisabled, JSON.stringify(c.body) + ` buttonsDisabled=${c.buttonsDisabled}`);
  check('Preview shows Would create / Would skip (reason), escaped; no reload', $('copy-result').textContent.includes('Would create') && $('copy-result').textContent.includes('Would skip') && $('copy-result').textContent.includes('c8 (exists (id 100) <b>)') && !$('copy-result').querySelector('img, b') && configGets === g0 && !$('btn-copy-run').disabled,
        $('copy-result').textContent.replace(/\s+/g, ' ').trim());

  copyReply = { preview: false, created: [{ id: 101, alert_type: 'broker.cpu_pct', cluster_id: 9, cluster_name: 'c9x', name: 'Broker CPU % - c9x' }],
                skipped: [{ alert_type: 'broker.cpu_pct', cluster_id: 8, cluster_name: 'c8', reason: 'exists (id 100)' }] };
  $('btn-copy-run').click(); await flush();
  c = calls[calls.length - 1];
  check('Copy sends preview false and reloads the list', JSON.stringify(c.body) === '{"config_id":20,"target_cluster_ids":[8,9],"preview":false}' && configGets === g0 + 1 && c.buttonsDisabled, JSON.stringify(c.body) + ` configs reloaded=${configGets - g0}`);
  check('Copy shows result and configs-msg', $('copy-result').textContent.includes('Created') && $('copy-result').textContent.includes('c9x') && $('configs-msg').textContent === '✓ Copied 1 rule(s)', `configs-msg='${$('configs-msg').textContent}' | ${$('copy-result').textContent.replace(/\s+/g, ' ').trim()}`);

  copyReply = () => json({ detail: 'Only cluster rules can be copied' }, 422);
  $('btn-copy-run').click(); await flush();
  check('API error shown inline, err style, buttons re-enabled', $('copy-msg').textContent === '✗ Copy failed: Only cluster rules can be copied' && $('copy-msg').classList.contains('err') && !$('btn-copy-run').disabled, `msg='${$('copy-msg').textContent}'`);
  await new Promise(r => setTimeout(r, 5200));
  check('error not auto-hidden after 5 s', $('copy-msg').textContent.includes('Copy failed'));

  copyBtn(21).click();
  check('reopen for heap (21): result cleared, boxes unchecked, source escaped as text', $('copy-result').innerHTML === '' && targets().every(b => !b.checked) && $('copy-msg').classList.contains('hidden') && !$('copy-source').querySelector('script'), $('copy-source').textContent);
  $('btn-copy-cancel').click();
  check('Cancel hides the panel', $('copy-panel').classList.contains('hidden'));
  console.log(fails ? `${fails} FAILED` : 'ALL PASS');
})();
