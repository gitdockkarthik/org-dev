const { TEAMS_HTML } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');

const calls = [];
const clusters = [{ id: 4, name: 'c4', enabled: true }, { id: 8, name: 'c8', enabled: true }];
const base = { severity: 'warning', threshold: 1, send_resolve_card: true, webhook_url: null, cooldown_minutes: 30, email_enabled: false, open_trigger_count: 0, min_rf: null };
let configs = [];
for (let i = 0; i < 12; i++) configs.push({ ...base, id: 100 + i, name: `CPU ${i}`, alert_type: 'broker.cpu_pct', cluster_id: 4, cluster_name: 'c' + String(i).padStart(2, '0'), config: { mode: 'simple', warning: 70 }, enabled: i % 2 === 0, email_enabled: i === 1 });
configs.push({ ...base, id: 3, name: 'BU', alert_type: 'broker_unreachable', cluster_id: null, cluster_name: 'All clusters', config: { threshold: 1 }, enabled: true });
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });
const dom = new JSDOM(html, {
  runScripts: 'dangerously', url: 'http://localhost/',
  beforeParse(w) {
    w.Element.prototype.scrollIntoView = () => {};
    w.mountUserBadge = () => {}; w.fetchCurrentUser = async () => null; w.confirm = () => true;
    w.fetch = async (url, opts = {}) => {
      const method = opts.method || 'GET';
      calls.push({ method, url, body: opts.body ? JSON.parse(opts.body) : null });
      if (url.endsWith('/settings/clusters')) return json({ clusters });
      if (url.includes('/alerts/triggers')) return json({ triggers: [], total_count: 0 });
      if (url.endsWith('/alerts/configs') && method === 'GET') return json({ configs });
      const m = url.match(/\/alerts\/configs\/(\d+)$/);
      if (m && method === 'DELETE') { configs = configs.filter(c => c.id !== Number(m[1])); return json({ ok: true }); }
      if (m && method === 'PUT') { Object.assign(configs.find(x => x.id === Number(m[1])), calls.at(-1).body); return json({ config: {} }); }
      return json({ config: {} });
    };
  },
});
const w = dom.window, d = w.document, $ = id => d.getElementById(id);
const flush = () => new Promise(r => setTimeout(r, 30));
let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '  ' + extra : ''}`); };
const row = id => d.querySelector(`#configs-body button.config-edit[data-id="${id}"]`)?.closest('tr');
const pagers = () => ['', '-top'].map(sfx => ({ hidden: $('configs-pager' + sfx).classList.contains('hidden'), label: $('configs-page-label' + sfx).textContent,
  prev: $('btn-configs-prev' + sfx).disabled, next: $('btn-configs-next' + sfx).disabled }));
const inSync = () => { const [b, t] = pagers(); return JSON.stringify(b) === JSON.stringify(t); };

(async () => {
  await flush();
  // CSS / structure
  const css = d.querySelector('style').textContent;
  check('.container has no width cap', /\.container \{ max-width: none; margin: 0 auto; \}/.test(css) && !css.includes('max-width: 1100px'));
  const table = $('configs-body').closest('table');
  const cols = [...table.querySelectorAll('colgroup col')].map(c => c.style.width || 'auto');
  check('rules table: fixed layout class, 10 cols matching 10 headers', table.classList.contains('configs-table') && cols.length === 10 && table.querySelectorAll('thead th').length === 10, cols.join(' '));
  check('Recent Triggers table untouched (no colgroup, no configs-table)', !$('triggers-body').closest('table').querySelector('colgroup') && !$('triggers-body').closest('table').classList.contains('configs-table'));

  // Row cells
  const r = row(100), tds = r.querySelectorAll('td');
  check('10 cells per row, colspan rows still 10', tds.length === 10);
  check('Type cell no longer nowrap', !tds[1].classList.contains('nowrap') && tds[1].textContent.startsWith('Broker CPU %'));
  const en = tds[6].querySelector('input'), em = tds[7].querySelector('input');
  const TEAMS_TIP = 'Posts to the Teams channel when this rule fires, escalates or resolves.';
  check('Teams toggle: same classes and data-id, switch only, state in title',
        en.className === 'toggle-input config-enable-toggle' && en.dataset.id === '100' && en.checked && !tds[6].querySelector('.toggle-text') && tds[6].textContent.trim() === ''
        && tds[6].querySelector('label').title === 'Enabled. ' + TEAMS_TIP && row(101).querySelectorAll('td')[6].querySelector('label').title === 'Disabled. ' + TEAMS_TIP);
  const EMAIL_TIP = 'Sends an email to the alert recipients when this rule fires, escalates or resolves. A rule with Teams and Email both off is not evaluated.';
  check('Email toggle: same classes and data-id, switch only, state + tooltip',
        em.className === 'toggle-input config-email-toggle' && em.dataset.id === '100' && !em.checked && !tds[7].querySelector('.toggle-text')
        && tds[7].querySelector('label').title === 'Disabled. ' + EMAIL_TIP
        && row(101).querySelectorAll('td')[7].querySelector('label').title === 'Enabled. ' + EMAIL_TIP);
  const acts = tds[9].querySelector('.configs-actions');
  const btns = acts ? [...acts.children].map(b => `${b.className}#${b.dataset.id}`) : [];
  check('Actions: Copy, Edit, Delete in one flex container, same classes and data-ids',
        JSON.stringify(btns) === JSON.stringify(['btn btn-secondary btn-sm config-copy#100', 'btn btn-secondary btn-sm config-edit#100', 'btn btn-danger btn-sm config-delete#100'])
        && acts.children[2].dataset.name === 'CPU 0', JSON.stringify(btns));

  // Pagers
  check('both pagers shown, same label/states on page 1', inSync() && pagers()[0].label === 'Showing 1-10 of 13' && pagers()[1].prev && !pagers()[1].next && !pagers()[1].hidden, JSON.stringify(pagers()));
  $('btn-configs-next-top').click();
  check('top Next -> page 2, both in sync', inSync() && pagers()[0].label === 'Showing 11-13 of 13' && pagers()[0].next && !pagers()[0].prev, JSON.stringify(pagers()));
  check('general rule row (page 2): Edit and Delete only', [...row(3).querySelector('.configs-actions').children].map(b => b.textContent).join(',') === 'Edit,Delete');
  $('btn-configs-prev').click();
  check('bottom Previous -> page 1, both in sync', inSync() && pagers()[1].label === 'Showing 1-10 of 13');
  $('configs-page-size').value = '25'; $('configs-page-size').dispatchEvent(new w.Event('change'));
  check('size 25: both pagers say 1-13, both disabled', inSync() && pagers()[0].label === 'Showing 1-13 of 13' && pagers()[0].prev && pagers()[0].next);
  $('configs-page-size').value = '10'; $('configs-page-size').dispatchEvent(new w.Event('change'));

  // Handlers
  row(100).querySelector('.config-enable-toggle').checked = false;
  row(100).querySelector('.config-enable-toggle').dispatchEvent(new w.Event('change')); await flush();
  check('Teams toggle change PUTs enabled:false', calls.some(c => c.method === 'PUT' && c.url.endsWith('/100') && c.body.enabled === false) && !row(100).querySelector('.config-enable-toggle').checked);
  row(100).querySelector('.config-email-toggle').checked = true;
  row(100).querySelector('.config-email-toggle').dispatchEvent(new w.Event('change')); await flush();
  check('Email toggle change PUTs email_enabled:true', calls.some(c => c.method === 'PUT' && c.url.endsWith('/100') && c.body.email_enabled === true));
  row(102).querySelector('.config-edit').click();
  check('Edit opens the form for that rule', $('af-name').value === 'CPU 2' && !$('alert-form-card').classList.contains('hidden'));
  $('btn-cancel-alert').click();
  row(102).querySelector('.config-copy').click();
  check('Copy opens the panel for that rule', $('copy-source').textContent.startsWith('Source: CPU 2 '));
  $('btn-copy-cancel').click();
  row(102).querySelector('.config-delete').click(); await flush(); await flush();
  check('Delete sends DELETE and the row is gone', calls.some(c => c.method === 'DELETE' && c.url.endsWith('/102')) && !row(102) && inSync() && pagers()[0].label === 'Showing 1-10 of 12');

  configs = []; await w.loadConfigs();
  check('no rules: both pagers hidden', pagers().every(p => p.hidden));

  console.log(fails ? `${fails} FAILED` : 'ALL PASSED');
  process.exit(fails ? 1 : 0);
})();
