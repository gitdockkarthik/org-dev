// ── Runtime config ─────────────────────────────────────────────────────────────
// BACKEND_URL injected by docker-entrypoint.sh; falls back to localhost for dev.
// API key is bootstrapped from the backend at load time — not from env vars.
const BACKEND_URL = '';
window._isPortalEnv = true;
let _apiKey = '';

// ── Agent accent colours ────────────────────────────────────────────────────────
const ACCENT_CLASSES = ['ca-0','ca-1','ca-2','ca-3','ca-4','ca-5','ca-6','ca-7'];

function agentAccentClass(slug) {
  let h = 0;
  for (let i = 0; i < slug.length; i++) h = (Math.imul(31, h) + slug.charCodeAt(i)) | 0;
  return ACCENT_CLASSES[Math.abs(h) % ACCENT_CLASSES.length];
}

// ── Session management ──────────────────────────────────────────────────────────
function generateSessionId() {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) return crypto.randomUUID();
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

function getOrCreateSession(agentSlug) {
  const key = `session_${agentSlug}`;
  let sid = sessionStorage.getItem(key);
  if (!sid) { sid = generateSessionId(); sessionStorage.setItem(key, sid); }
  return sid;
}

function resetSession(agentSlug) {
  const key = `session_${agentSlug}`;
  const sid = generateSessionId();
  sessionStorage.setItem(key, sid);
  return sid;
}

// ── HTTP helpers ────────────────────────────────────────────────────────────────
async function _json(res) {
  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(body.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

function _authHeaders() {
  return _apiKey ? { 'X-API-Key': _apiKey } : {};
}

function getApiKey() { return _apiKey; }

async function initPortal() {
  try {
    const data = await _json(await fetch(`${BACKEND_URL}/api/platform/bootstrap`));
    _apiKey = data.api_key || '';
  } catch (_) { /* backend unreachable — proceed without key */ }
}

// Auto-bootstrap: fetch API key from backend on script load.
// Expose as a Promise so pages can await portalReady before making authenticated calls.
const portalReady = initPortal();

// ── Current user cache ──────────────────────────────────────────────────────
let _currentUser = null;

async function fetchCurrentUser() {
  if (_currentUser) return _currentUser;
  try {
    const res = await fetch('/api/auth/me', { credentials: 'include' });
    if (!res.ok) return null;
    _currentUser = await res.json();
    return _currentUser;
  } catch {
    return null;
  }
}

// ── Auth gate ───────────────────────────────────────────────────────────────
async function requireAuth() {
  const mode = window.__CONFIG__?.AUTH_MODE || 'none';
  if (mode !== 'local') return null;
  const user = await fetchCurrentUser();
  if (!user) {
    window.location.replace('/login');
    return null;
  }
  return user;
}

// ── User badge ──────────────────────────────────────────────────────────────
function mountUserBadge(slotSelector) {
  const mode = window.__CONFIG__?.AUTH_MODE || 'none';
  const slot = document.querySelector(slotSelector);
  if (!slot) return;
  if (mode !== 'local') { slot.style.display = 'none'; return; }

  fetchCurrentUser().then(user => {
    if (!user) return;
    const initial = (user.name || user.email || '?')[0].toUpperCase();
    const roles = (user.roles || 'user').split(',').map(r => r.trim());
    const rolePills = roles.map(r =>
      `<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72rem;font-weight:600;background:${
        r === 'admin' ? 'rgba(16,185,129,.15)' : r === 'developer' ? 'rgba(99,102,241,.15)' : 'rgba(217,119,6,.15)'
      };color:${
        r === 'admin' ? '#10b981' : r === 'developer' ? '#6366f1' : '#d97706'
      }">${r}</span>`
    ).join(' ');

    slot.innerHTML = `
      <div style="position:relative;display:inline-block;">
        <button id="user-badge-btn" style="width:32px;height:32px;border-radius:50%;background:var(--accent);border:none;color:#fff;font-weight:700;font-size:.88rem;cursor:pointer;display:flex;align-items:center;justify-content:center;" onclick="document.getElementById('user-badge-dropdown').classList.toggle('open')">
          ${initial}
        </button>
        <div id="user-badge-dropdown" style="display:none;position:absolute;right:0;top:40px;background:#ffffff;border:1px solid #e2e8f0;border-radius:8px;padding:14px 16px;min-width:220px;z-index:1000;box-shadow:0 4px 16px rgba(0,0,0,.15);">
          <div style="font-weight:600;color:#1e293b;font-size:.9rem;margin-bottom:2px;">${user.name}</div>
          <div style="color:#64748b;font-size:.78rem;margin-bottom:8px;">${user.email}</div>
          <div style="margin-bottom:12px;">${rolePills}</div>
          <hr style="border:none;border-top:1px solid #e2e8f0;margin-bottom:10px;">
          <a href="#" onclick="openChangePasswordModal();return false;" style="display:block;color:#475569;font-size:.83rem;margin-bottom:8px;text-decoration:none;">🔑 Change Password</a>
          <a href="#" onclick="portalLogout();return false;" style="display:block;color:var(--danger, #dc2626);font-size:.83rem;text-decoration:none;">Sign out</a>
        </div>
      </div>`;

    // Close dropdown on outside click
    document.addEventListener('click', e => {
      const btn = document.getElementById('user-badge-btn');
      const dd = document.getElementById('user-badge-dropdown');
      if (dd && btn && !btn.contains(e.target) && !dd.contains(e.target)) {
        dd.classList.remove('open');
        dd.style.display = 'none';
      }
    });

    // Toggle display on open class
    const observer = new MutationObserver(() => {
      const dd = document.getElementById('user-badge-dropdown');
      if (dd) dd.style.display = dd.classList.contains('open') ? 'block' : 'none';
    });
    const dd = document.getElementById('user-badge-dropdown');
    if (dd) observer.observe(dd, { attributes: true, attributeFilter: ['class'] });
  });
}

async function portalLogout() {
  await fetch('/api/auth/logout', { method: 'POST', credentials: 'include' });
  window.location.href = '/login';
}

// ── Change Password Modal ───────────────────────────────────────────────────
function openChangePasswordModal() {
  const dd = document.getElementById('user-badge-dropdown');
  if (dd) { dd.classList.remove('open'); dd.style.display = 'none'; }

  let modal = document.getElementById('change-pw-modal');
  if (!modal) {
    modal = document.createElement('div');
    modal.id = 'change-pw-modal';
    modal.style.cssText = 'display:none;position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:2000;align-items:center;justify-content:center;';
    modal.innerHTML = `
      <div style="background:var(--navy-1);border-radius:8px;padding:28px;width:360px;border:1px solid var(--border);">
        <h3 style="margin:0 0 18px 0;font-size:1rem;color:var(--text);">Change Password</h3>
        <div style="margin-bottom:12px;">
          <label style="display:block;font-size:.83rem;font-weight:600;color:var(--text-muted);margin-bottom:6px;">Current Password</label>
          <input id="cp-current" type="password" style="width:100%;padding:8px 12px;background:var(--navy-2);border:1.5px solid var(--border-hi);border-radius:6px;color:var(--text);font-size:.88rem;box-sizing:border-box;">
        </div>
        <div style="margin-bottom:12px;">
          <label style="display:block;font-size:.83rem;font-weight:600;color:var(--text-muted);margin-bottom:6px;">New Password</label>
          <input id="cp-new" type="password" style="width:100%;padding:8px 12px;background:var(--navy-2);border:1.5px solid var(--border-hi);border-radius:6px;color:var(--text);font-size:.88rem;box-sizing:border-box;">
        </div>
        <div style="margin-bottom:16px;">
          <label style="display:block;font-size:.83rem;font-weight:600;color:var(--text-muted);margin-bottom:6px;">Confirm New Password</label>
          <input id="cp-confirm" type="password" style="width:100%;padding:8px 12px;background:var(--navy-2);border:1.5px solid var(--border-hi);border-radius:6px;color:var(--text);font-size:.88rem;box-sizing:border-box;">
        </div>
        <div style="display:flex;gap:10px;align-items:center;">
          <button onclick="submitChangePassword()" style="padding:9px 20px;background:var(--accent);color:#fff;border:none;border-radius:6px;font-size:.88rem;font-weight:600;cursor:pointer;">Save</button>
          <button onclick="document.getElementById('change-pw-modal').style.display='none'" style="padding:9px 16px;background:transparent;color:var(--text-muted);border:1.5px solid var(--border-hi);border-radius:6px;font-size:.88rem;cursor:pointer;">Cancel</button>
          <span id="cp-status" style="font-size:.83rem;"></span>
        </div>
      </div>`;
    document.body.appendChild(modal);
  }
  modal.style.display = 'flex';
}

async function submitChangePassword() {
  const current = document.getElementById('cp-current').value;
  const newPw = document.getElementById('cp-new').value;
  const confirm = document.getElementById('cp-confirm').value;
  const statusEl = document.getElementById('cp-status');
  statusEl.textContent = 'Saving…';
  statusEl.style.color = 'var(--text-muted)';
  try {
    const r = await fetch('/api/auth/me/password', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify({ current_password: current, new_password: newPw, confirm_password: confirm }),
    });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    statusEl.textContent = '✓ Password changed';
    statusEl.style.color = 'var(--success, #16a34a)';
    setTimeout(() => { document.getElementById('change-pw-modal').style.display = 'none'; }, 1500);
  } catch (err) {
    statusEl.textContent = err.message;
    statusEl.style.color = 'var(--danger, #dc2626)';
  }
}

// ── Public API ──────────────────────────────────────────────────────────────────

/** Fetch published agents (no auth required). */
async function fetchAgents() {
  return _json(await fetch(`${BACKEND_URL}/api/agents`));
}

/** Fetch all agents including drafts (requires X-API-Key). */
async function fetchAllAgents({ status } = {}) {
  const qs = status ? `?status=${encodeURIComponent(status)}` : '';
  return _json(await fetch(`${BACKEND_URL}/api/registry/agents${qs}`, {
    headers: _authHeaders(),
    credentials: 'include',
  }));
}

/** Get a single published agent by slug. */
async function fetchAgent(slug) {
  return _json(await fetch(`${BACKEND_URL}/api/agents/${encodeURIComponent(slug)}`));
}

/** Register a new agent manifest. */
async function registerAgent(data) {
  return _json(await fetch(`${BACKEND_URL}/api/registry/agents`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ..._authHeaders() },
    body: JSON.stringify(data),
  }));
}

/** Publish an agent by registry ID. */
async function publishAgent(agentId) {
  return _json(await fetch(`${BACKEND_URL}/api/registry/agents/${agentId}/publish`, {
    method: 'POST',
    headers: _authHeaders(),
  }));
}

/** Deprecate an agent by registry ID. */
async function deprecateAgent(agentId) {
  return _json(await fetch(`${BACKEND_URL}/api/registry/agents/${agentId}/deprecate`, {
    method: 'POST',
    headers: _authHeaders(),
  }));
}

/** Update an agent's editable fields by registry ID. */
async function updateAgent(agentId, data) {
  return _json(await fetch(`${BACKEND_URL}/api/registry/agents/${agentId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json', ..._authHeaders() },
    body: JSON.stringify(data),
  }));
}

/** Permanently delete an agent by registry ID. */
async function deleteAgent(agentId) {
  const res = await fetch(`${BACKEND_URL}/api/registry/agents/${agentId}`, {
    method: 'DELETE',
    headers: _authHeaders(),
  });
  if (!res.ok) {
    const e = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(e.detail || `HTTP ${res.status}`);
  }
}

/** Fetch chat history for a session UUID. */
async function getHistory(sessionId) {
  return _json(await fetch(`${BACKEND_URL}/api/session/${encodeURIComponent(sessionId)}`));
}

/**
 * Invoke an agent with streaming display.
 *
 * Reads the response body via ReadableStream. The current backend returns
 * full JSON (ready for SSE when a /stream endpoint is added). Displays the
 * assistant text character-by-character via requestAnimationFrame.
 *
 * @param {string}   slug       — agent slug
 * @param {string}   message    — user message
 * @param {string}   sessionId  — UUID string
 * @param {Object}   context    — optional per-agent context (report data, etc.)
 * @param {Function} onChunk    — called with each character as it renders
 * @param {Function} onDone     — called with the full response object when complete
 * @param {Function} onError    — called with an Error on failure
 */
async function invokeAgent(slug, message, sessionId, context, onChunk, onDone, onError) {
  // Guard: if a caller passes callbacks without the context arg the names shift by one.
  // Fail fast here instead of getting a cryptic "onChunk is not a function" inside tick().
  if (typeof onChunk !== 'function' || typeof onDone !== 'function' || typeof onError !== 'function') {
    const types = { context: typeof context, onChunk: typeof onChunk, onDone: typeof onDone, onError: typeof onError };
    console.error('[invokeAgent] wrong call signature — expected (slug, message, sessionId, context, onChunk, onDone, onError). Received types:', types);
    throw new TypeError('[invokeAgent] onChunk, onDone and onError must all be functions. Did you forget the context argument?');
  }
  console.log('[invokeAgent] called — slug:', slug, 'context keys:', Object.keys(context || {}));

  let res;
  try {
    res = await fetch(`${BACKEND_URL}/api/agents/${encodeURIComponent(slug)}/proxy/invoke/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ..._authHeaders() },
      body: JSON.stringify({
        session_id: sessionId,
        user_message: message,
        context: context || {},
        history: [],
      }),
    });
  } catch (err) {
    onError(err);
    return;
  }
  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    onError(new Error(body.detail || `HTTP ${res.status}`));
    return;
  }
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const lines = decoder.decode(value, { stream: true }).split('\n');
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        const data = line.slice(6);
        if (data === '[DONE]') { onDone({}); return; }
        if (data.startsWith('[STOP_REASON]')) continue;
        if (data.startsWith('[ERROR]')) { onError(new Error(data.slice(7).trim())); return; }
        const text = data.replace(/\\n/g, '\n');
        onChunk(text);
      }
    }
  } catch (err) {
    onError(err);
  }
}

// ── Markdown renderer ───────────────────────────────────────────────────────────
// Handles the most common Claude output patterns without an external library.
function renderMarkdown(raw) {
  let s = raw
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

  // Fenced code blocks
  s = s.replace(/```(\w*)\n?([\s\S]*?)```/g, (_, lang, code) => {
    const trimmed = code.trim();
    // Detect Chart.js JSON blobs and render as table
    if (lang === 'json' || trimmed.startsWith('{')) {
      try {
        const obj = JSON.parse(trimmed);
        if (obj.type && obj.labels && obj.datasets) {
          const labels = obj.labels;
          const datasets = obj.datasets;
          let html = '<table class="md-table"><thead><tr><th>Label</th>';
          datasets.forEach(ds => {
            html += `<th>${ds.label || 'Value'}</th>`;
          });
          html += '</tr></thead><tbody>';
          labels.forEach((lbl, i) => {
            html += `<tr><td>${lbl}</td>`;
            datasets.forEach(ds => {
              const val = ds.data?.[i];
              html += `<td>${val != null ? (typeof val === 'number' ? val.toLocaleString() : val) : '—'}</td>`;
            });
            html += '</tr>';
          });
          html += '</tbody></table>';
          return html;
        }
      } catch(e) { /* not JSON — fall through to code block */ }
    }
    return `<pre><code class="lang-${lang}">${trimmed}</code></pre>`;
  });

  // Bare chart JSON blobs (not in code fences)
  s = s.replace(/^\s*(\{"type":"(?:bar|line|doughnut)"[\s\S]*?\})\s*$/gm, (_, json) => {
    try {
      const obj = JSON.parse(json);
      if (obj.type && obj.labels && obj.datasets) {
        const labels = obj.labels;
        const datasets = obj.datasets;
        let html = '<table class="md-table"><thead><tr><th>Label</th>';
        datasets.forEach(ds => {
          html += `<th>${ds.label || 'Value'}</th>`;
        });
        html += '</tr></thead><tbody>';
        labels.forEach((lbl, i) => {
          html += `<tr><td>${lbl}</td>`;
          datasets.forEach(ds => {
            const val = ds.data?.[i];
            html += `<td>${val != null ? (typeof val === 'number' ? val.toLocaleString() : val) : '—'}</td>`;
          });
          html += '</tr>';
        });
        html += '</tbody></table>';
        return html;
      }
    } catch(e) {}
    return _;
  });
  // Inline code
  s = s.replace(/`([^`]+)`/g, '<code>$1</code>');
  // Bold / italic
  s = s.replace(/\*\*\*(.+?)\*\*\*/g, '<strong><em>$1</em></strong>');
  s = s.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  s = s.replace(/\*([^*\n]+)\*/g, '<em>$1</em>');
  // Headers
  s = s.replace(/^### (.+)$/gm, '<h3>$1</h3>');
  s = s.replace(/^## (.+)$/gm,  '<h2>$1</h2>');
  s = s.replace(/^# (.+)$/gm,   '<h1>$1</h1>');
  // Tables — header row, separator row (|---|), then body rows
  s = s.replace(
    /^([ \t]*\|[^\n]+\|[ \t]*)\n[ \t]*\|[-: |]+\|\n((?:[ \t]*\|[^\n]+\|[ \t]*\n?)*)/gm,
    (_, headerLine, bodyLines) => {
      const parseRow = row =>
        row.replace(/^[ \t]*\|/, '').replace(/\|[ \t]*$/, '').split('|').map(c => c.trim());
      const headers = parseRow(headerLine);
      const rows = bodyLines.trim()
        ? bodyLines.trim().split('\n').map(parseRow)
        : [];
      let html = '<table class="md-table"><thead><tr>';
      html += headers.map(h => `<th>${h}</th>`).join('');
      html += '</tr></thead>';
      if (rows.length) {
        html += '<tbody>';
        for (const cells of rows) {
          html += '<tr>' + cells.map(c => `<td>${c}</td>`).join('') + '</tr>';
        }
        html += '</tbody>';
      }
      html += '</table>';
      return html;
    }
  );
  // Unordered lists
  s = s.replace(/^[-*] (.+)$/gm, '<li>$1</li>');
  s = s.replace(/(<li>.*<\/li>\n?)+/g, m => `<ul>${m}</ul>`);
  // Ordered lists
  s = s.replace(/^\d+\. (.+)$/gm, '<li>$1</li>');
  // Horizontal rule
  s = s.replace(/^---+$/gm, '<hr>');
  // Paragraphs (double newlines outside blocks)
  s = s.replace(/\n{2,}/g, '</p><p>');
  s = s.replace(/\n/g, '<br>');
  return `<p>${s}</p>`;
}

// ── Toast notifications ─────────────────────────────────────────────────────────
let _toastContainer;
function _ensureToasts() {
  if (!_toastContainer) {
    _toastContainer = document.createElement('div');
    _toastContainer.className = 'toast-container';
    document.body.appendChild(_toastContainer);
  }
}

function showToast(message, type = 'success') {
  _ensureToasts();
  const el = document.createElement('div');
  el.className = `toast toast--${type}`;
  el.innerHTML = `<span class="toast__dot"></span>
    <span class="toast__text">${message}</span>
    <button class="toast__close" aria-label="Dismiss">×</button>`;
  el.querySelector('.toast__close').onclick = () => el.remove();
  _toastContainer.appendChild(el);
  setTimeout(() => el.remove(), 4500);
}

// ── Misc helpers ────────────────────────────────────────────────────────────────
function formatTime(isoString) {
  if (!isoString) return '';
  try {
    return new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit' }).format(new Date(isoString));
  } catch { return ''; }
}

function truncate(str, n) {
  return str && str.length > n ? str.slice(0, n) + '…' : str;
}
