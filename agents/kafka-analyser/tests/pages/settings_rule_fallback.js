// settings.html: the rule_teams_fallback checkbox (rule notices through the
// outbox, step 1). Loaded from GET /settings (missing = on) and sent by Save.
const { SETTINGS_HTML } = require('./_page');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const html = fs.readFileSync(SETTINGS_HTML, 'utf8');

let fails = 0;
const check = (label, cond, extra = '') => { if (!cond) fails++; console.log(`[${cond ? 'PASS' : 'FAIL'}] ${label}${extra ? '  ' + extra : ''}`); };
const flush = () => new Promise(r => setTimeout(r, 50));
const json = (body, status = 200) => ({ ok: status < 400, status, json: async () => body, text: async () => JSON.stringify(body) });

async function load(cfg) {
  const posts = [];
  const dom = new JSDOM(html, { runScripts: 'dangerously', url: 'http://localhost/', beforeParse(w) {
    w.Element.prototype.scrollIntoView = () => {};
    w.mountUserBadge = () => {}; w.fetchCurrentUser = async () => null;
    w.fetch = async (url, opts = {}) => {
      if ((opts.method || 'GET') === 'POST') { posts.push({ url, body: JSON.parse(opts.body || '{}') }); return json({ ok: true }); }
      if (/\/settings$/.test(url)) return json(cfg);
      if (/\/settings\/clusters/.test(url)) return json({ clusters: [] });
      return json({});
    };
  }});
  await flush(); await flush();
  return { w: dom.window, $: id => dom.window.document.getElementById(id), posts };
}

(async () => {
  let p = await load({ email_recipients: 'rules@operative.com' });
  const box = p.$('rule-teams-fallback');
  check('checkbox present, in the Email Escalation card, after the alert rule recipients',
    !!box && box.type === 'checkbox' && box.closest('.card').querySelector('.card-section-title').textContent === 'Email Escalation'
    && !!(p.$('email-recipients').compareDocumentPosition(box) & p.w.Node.DOCUMENT_POSITION_FOLLOWING));
  check('label text', box.closest('label').textContent.trim() === 'If an email cannot be delivered, post it to Teams. Teams must be on in Settings.', box.closest('label').textContent.trim());
  check('setting missing from GET /settings: ticked (default on)', box.checked === true);
  p.$('btn-save-thresholds').click(); await flush();
  let post = p.posts.find(x => /\/settings$/.test(x.url));
  check('Save sends rule_teams_fallback true', post && post.body.rule_teams_fallback === true, JSON.stringify(post && post.body));

  p = await load({ rule_teams_fallback: false });
  check('setting false: unticked', p.$('rule-teams-fallback').checked === false);
  p.$('btn-save-thresholds').click(); await flush();
  post = p.posts.find(x => /\/settings$/.test(x.url));
  check('Save sends rule_teams_fallback false, and no evaluator list', post && post.body.rule_teams_fallback === false && !('rule_outbox_evaluators' in post.body), JSON.stringify(post && post.body));
  check('no control for the evaluator list', !/rule_outbox_evaluators|rule-outbox-evaluators/.test(html));
  console.log(fails ? `${fails} FAILED` : 'ALL PASS');
  process.exit(fails ? 1 : 0);
})();
