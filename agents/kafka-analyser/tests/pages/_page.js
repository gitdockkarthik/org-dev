// Shared by the page stubs: file locations, and expectations derived from
// the page's own ALERT_TYPES so that adding an alert type does not break them.
const path = require('path');

const REPO = path.resolve(__dirname, '..', '..', '..', '..');
const PORTAL = path.join(REPO, 'portal', 'agents', 'kafka-analyser');
const TEAMS_HTML = process.env.TEAMS_HTML || path.join(PORTAL, 'teams.html');  // override to test a candidate page
const SETTINGS_HTML = path.join(PORTAL, 'settings.html');
const ROUTES_ALERTS = path.join(REPO, 'agents', 'kafka-analyser', 'routes_alerts.py');

// The page's ALERT_TYPES (a top-level const of its inline script).
function alertTypes(w) {
  return w.eval('ALERT_TYPES').map(t => ({ ...t }));
}

// Option values of one optgroup of #af-alert-type, in page order.
function groupValues(w, group) {
  const grp = w.document.getElementById('af-alert-type').querySelector(`optgroup[label="${group}"]`);
  return grp ? [...grp.querySelectorAll('option')].map(o => o.value) : [];
}

// The optgroup shows exactly ALERT_TYPES of that group, in order, with their labels.
function groupMatchesAlertTypes(w, group) {
  const grp = w.document.getElementById('af-alert-type').querySelector(`optgroup[label="${group}"]`);
  const got = grp ? [...grp.querySelectorAll('option')].map(o => `${o.value}=${o.textContent}`) : [];
  const want = alertTypes(w).filter(t => t.group === group).map(t => `${t.value}=${t.label}`);
  return JSON.stringify(got) === JSON.stringify(want);
}

// True when seq appears in list as one consecutive run, in this order.
function consecutive(list, seq) {
  const i = list.indexOf(seq[0]);
  return i >= 0 && seq.every((v, k) => list[i + k] === v);
}

// Tier hint rules, checked for every type in the select (setType selects one):
//  - af-tiers-above-hint is shown exactly for the types without below;
//  - every below type shows exactly one other tier hint (its own below-style
//    hint), never the above hint;
//  - every other tier hint is shown for at most one type;
//  - owners ({hintId: typeValue}) are exact: shown for that type and no other.
// Returns a list of problems ("type:hintId ..."), empty when all hold.
function tierHintProblems(w, setType, owners = {}) {
  const d = w.document, $ = id => d.getElementById(id);
  const shown = id => !$(id).classList.contains('hidden');
  const hintIds = [...d.querySelectorAll('#af-tiers-field .hint[id]')].map(h => h.id).filter(id => id !== 'af-tiers-above-hint');
  const byType = Object.fromEntries(alertTypes(w).map(t => [t.value, t]));
  const seenFor = {};
  const bad = [];
  for (const o of $('af-alert-type').options) {
    setType(o.value);
    const t = byType[o.value] || {};
    if (shown('af-tiers-above-hint') !== !t.below) bad.push(`${o.value}:above`);
    const visible = hintIds.filter(shown);
    if (t.below && visible.length !== 1) bad.push(`${o.value}:below-style hints ${JSON.stringify(visible)}`);
    for (const h of visible) (seenFor[h] = seenFor[h] || []).push(o.value);
    for (const [h, owner] of Object.entries(owners)) if (shown(h) !== (o.value === owner)) bad.push(`${o.value}:${h}`);
  }
  for (const [h, types] of Object.entries(seenFor)) if (types.length > 1) bad.push(`${h} shown for ${types.join(',')}`);
  return bad;
}

module.exports = { REPO, TEAMS_HTML, SETTINGS_HTML, ROUTES_ALERTS, alertTypes, groupValues, groupMatchesAlertTypes, consecutive, tierHintProblems };
