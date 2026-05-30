#!/bin/sh
set -e

# ── Runtime defaults ──────────────────────────────────────────────────────────
export BACKEND_URL="${BACKEND_URL:-http://localhost:8000}"
export PORT="${PORT:-80}"

# ── Step 1: inject JS config ──────────────────────────────────────────────────
envsubst '${BACKEND_URL}' \
  < /etc/nginx/templates/config.template.js \
  > /usr/share/nginx/html/js/config.js

# ── Step 2: configure HTTP Basic Auth ────────────────────────────────────────
if [ -n "$PORTAL_USERNAME" ] && [ -n "$PORTAL_PASSWORD" ]; then
    printf '%s\n' "$PORTAL_PASSWORD" | htpasswd -i -c /etc/nginx/.htpasswd "$PORTAL_USERNAME"
    export AUTH_REALM="AgentsIQ Studio"
    echo "  Auth = enabled (user: $PORTAL_USERNAME)"
else
    export AUTH_REALM="off"
    echo "  Auth = disabled (PORTAL_USERNAME/PORTAL_PASSWORD not set)"
fi

# ── Step 3: inject PORT and AUTH_REALM into nginx conf ───────────────────────
# Scope envsubst explicitly — nginx uses $uri, $host, etc. which must not expand.
envsubst '${PORT} ${AUTH_REALM}' \
  < /etc/nginx/templates/nginx.conf.template \
  > /etc/nginx/conf.d/default.conf

echo "Portal entrypoint complete:"
echo "  BACKEND_URL = $BACKEND_URL"
echo "  PORT        = $PORT"

exec nginx -g "daemon off;"
