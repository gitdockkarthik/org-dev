const { TEAMS_HTML } = require('./_page');
// Lag block layout (CSS only): computed styles and DOM order in jsdom (no layout engine).
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(TEAMS_HTML, 'utf8');
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });
const dom = new JSDOM(html, {
  runScripts: 'dangerously', url: 'http://localhost/',
  beforeParse(w) {
    w.Element.prototype.scrollIntoView = () => {};
    w.mountUserBadge = () => {}; w.fetchCurrentUser = async () => null;
    w.fetch = async (url) => {
      if (url.endsWith('/settings/clusters')) return json({ clusters: [{ id: 4, name: 'c4', enabled: true }] });
      if (url.includes('/alerts/triggers')) return json({ triggers: [], total_count: 0 });
      if (url.endsWith('/alerts/configs')) return json({ configs: [] });
      if (url.includes('/dashboard/consumer-groups')) return json({ consumer_groups: [{ group_id: 'aos-prod-orders-service-consumer-group-v2' }] });
      return json({});
    };
  },
});
const w = dom.window, d = w.document, $ = id => d.getElementById(id), cs = el => w.getComputedStyle(el);
let pass = 0, fail = 0;
const check = (l, c, x = '') => { c ? pass++ : fail++; console.log(`[${c ? 'PASS' : 'FAIL'}] ${l}${x ? '  ' + x : ''}`); };
(async () => {
  await new Promise(r => setTimeout(r, 30));
  check('#af-lag-field spans a full grid row (grid-column 1 / -1)', cs($('af-lag-field')).gridColumn === '1 / -1', cs($('af-lag-field')).gridColumn);
  check('#af-tiers-field still spans a full row', cs($('af-tiers-field')).gridColumn === '1 / -1');
  const cooldown = $('af-cooldown').closest('.field');
  check('Cooldown keeps its own cell (no grid-column), not inside the lag block', cs(cooldown).gridColumn === '' && !$('af-lag-field').contains(cooldown));
  const grid = $('af-lag-field').parentElement;
  const kids = [...grid.children];
  check('order in the grid: tiers, lag block, min RF, Cooldown', grid.classList.contains('field-grid')
    && kids.indexOf($('af-tiers-field')) + 1 === kids.indexOf($('af-lag-field'))
    && kids.indexOf($('af-lag-field')) < kids.indexOf($('af-min-rf-field')) && kids.indexOf($('af-min-rf-field')) < kids.indexOf(cooldown));
  // add a row through the real picker
  $('btn-new-alert').click();
  $('af-alert-type').value = 'cluster.consumer_lag'; $('af-alert-type').dispatchEvent(new w.Event('change'));
  $('af-cluster').value = '4'; $('af-cluster').dispatchEvent(new w.Event('change'));
  await new Promise(r => setTimeout(r, 30));
  $('af-lag-search').value = 'orders'; $('af-lag-search').dispatchEvent(new w.Event('input'));
  $('af-lag-picks').querySelector('button').click();
  const nameCell = d.querySelector('#af-lag-custom-body td.af-lag-name');
  const nameTh = d.querySelector('#af-lag-custom-table th');
  check('name column at least 16rem (th and td)', cs(nameCell).minWidth === '16rem' && cs(nameTh).minWidth === '16rem', cs(nameCell).minWidth);
  check('names wrap at hyphens, mid-word only if needed (word-break normal, overflow-wrap break-word)',
    cs(nameCell).wordBreak === 'normal' && cs(nameCell).overflowWrap === 'break-word', cs(nameCell).wordBreak + ' / ' + cs(nameCell).overflowWrap);
  const xCell = d.querySelector('#af-lag-custom-body tr td:last-child');
  check('× in its own shrink-to-fit cell (width 1%, nowrap)', xCell.querySelector('button.af-lag-row-remove') && cs(xCell).width === '1%' && cs(xCell).whiteSpace === 'nowrap');
  const wrap = $('af-lag-custom-table').parentElement;
  check('table inside a horizontally scrolling wrapper (max 44rem)', wrap.classList.contains('af-lag-table-wrap') && cs(wrap).overflowX === 'auto' && cs(wrap).maxWidth === '44rem');
  check('ids and handler classes unchanged; table shown with the row', !$('af-lag-custom-table').classList.contains('hidden') && !!$('af-lag-custom-body')
    && d.querySelectorAll('.af-lag-row-tier').length === 2 && !!d.querySelector('.af-lag-row-remove'));
  d.querySelector('.af-lag-row-remove').click();
  check('× still removes the row', d.querySelectorAll('#af-lag-custom-body tr').length === 0 && $('af-lag-custom-table').classList.contains('hidden'));
  console.log(`\n${pass}/${pass + fail} passed`); process.exit(fail ? 1 : 0);
})();
