#!/bin/bash
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

MONOREPO="${1:-../unified-agent-platform}"

echo ""
echo "═══════════════════════════════════════════"
echo "  Operative — Sync from AgentsIQ Monorepo"
echo "═══════════════════════════════════════════"
echo ""

# Validate monorepo exists
if [ ! -d "$MONOREPO" ]; then
  echo -e "${RED}✗ Monorepo not found at: $MONOREPO${NC}"
  echo "  Usage: ./sync-from-monorepo.sh [path-to-monorepo]"
  echo "  Default: ../unified-agent-platform"
  exit 1
fi

echo "Syncing from: $MONOREPO"
echo ""

# ─── Agents ────────────────────────────────────
echo "Syncing agents..."

sync_agent() {
  local agent=$1
  if [ -d "$MONOREPO/agents/$agent" ]; then
    cp -r "$MONOREPO/agents/$agent/" "agents/$agent/"
    # Remove Railway-specific files
    rm -f "agents/$agent/railway.toml"
    echo -e "${GREEN}✓ $agent synced${NC}"
  else
    echo -e "${YELLOW}⚠ $agent not found in monorepo — skipped${NC}"
  fi
}

sync_agent "alert-analyser"
sync_agent "cur-analyser"
sync_agent "infra-health"
sync_agent "kafka-analyser"

echo ""

# ─── Backend ───────────────────────────────────
echo "Syncing backend..."
if [ -d "$MONOREPO/backend" ]; then
  cp -r "$MONOREPO/backend/" "backend/"
  rm -f "backend/railway.toml"
  echo -e "${GREEN}✓ backend synced${NC}"
fi

echo ""

# ─── Portal — SELECTIVE sync ───────────────────
echo "Syncing portal (selective — preserving Operative branding)..."

# Safe to sync — no branding
cp "$MONOREPO/portal/nginx.conf" "portal/nginx.conf"
echo -e "${GREEN}✓ portal/nginx.conf${NC}"

# Fix BACKEND_URL to use relative URLs
cp "$MONOREPO/portal/js/portal.js" "portal/js/portal.js"
sed -i.bak "s/const BACKEND_URL = (window.__CONFIG__?.BACKEND_URL.*replace.*);/const BACKEND_URL = '';/" \
  "portal/js/portal.js"
rm -f "portal/js/portal.js.bak"
echo -e "${GREEN}✓ portal/js/portal.js (BACKEND_URL fixed)${NC}"

cp "$MONOREPO/portal/js/config.template.js" \
   "portal/js/config.template.js"
echo -e "${GREEN}✓ portal/js/config.template.js${NC}"

cp "$MONOREPO/portal/css/style.css" \
   "portal/css/style.css" 2>/dev/null && \
   echo -e "${GREEN}✓ portal/css/style.css${NC}" || true

# Sync portal agent pages (no branding in these)
cp -r "$MONOREPO/portal/agents/" "portal/agents/"
echo -e "${GREEN}✓ portal/agents/ pages${NC}"

# Fix AgentsIQ branding in all portal agent pages
echo ""
echo "Fixing OPERATIVE branding in portal pages..."
for f in portal/agents/*/**.html portal/chat.html; do
  if [ -f "$f" ]; then
    sed -i.bak "s|<img src='/assets/agentsiq-logo\.svg' alt='AgentsIQ' style='height:44px;'>|<span style=\"font-size:1.1rem;font-weight:700;color:#ffffff;letter-spacing:1px;font-family:inherit;\">OPERATIVE<\/span>|g" "$f"
    rm -f "$f.bak"
  fi
done
echo -e "${GREEN}✓ OPERATIVE branding applied to all portal pages${NC}"

# DO NOT sync — these have Operative branding:
echo -e "${YELLOW}⚠ Skipped (Operative branding):${NC}"
echo "  portal/index.html"
echo "  portal/setup.html"
echo "  portal/assets/"

echo ""

# ─── Shared ────────────────────────────────────
echo "Syncing shared..."
if [ -d "$MONOREPO/shared" ]; then
  cp -r "$MONOREPO/shared/" "shared/"
  echo -e "${GREEN}✓ shared synced${NC}"
fi

echo ""

# ─── Post-sync checks ──────────────────────────
echo "Running post-sync checks..."

# Ensure BACKEND_URL is relative in portal.js
if grep -q "BACKEND_URL = ''" portal/js/portal.js; then
  echo -e "${GREEN}✓ portal.js BACKEND_URL is relative${NC}"
else
  echo -e "${RED}✗ portal.js BACKEND_URL fix failed — fix manually${NC}"
fi

# Ensure nginx proxy block exists
if grep -q "proxy_pass" portal/nginx.conf; then
  echo -e "${GREEN}✓ nginx.conf proxy_pass present${NC}"
else
  echo -e "${RED}✗ nginx proxy_pass missing — check nginx.conf${NC}"
fi

# Ensure Operative branding intact
if grep -q "OPERATIVE" portal/index.html; then
  echo -e "${GREEN}✓ Operative branding intact in index.html${NC}"
else
  echo -e "${YELLOW}⚠ OPERATIVE not found in index.html — check branding${NC}"
fi

if grep -q "OPERATIVE" portal/setup.html; then
  echo -e "${GREEN}✓ Operative branding intact in setup.html${NC}"
else
  echo -e "${YELLOW}⚠ OPERATIVE not found in setup.html — check branding${NC}"
fi

echo ""
echo "═══════════════════════════════════════════"
echo -e "${GREEN}✓ Sync complete${NC}"
echo "═══════════════════════════════════════════"
echo ""
echo "Next steps:"
echo "  git status              ← review changes"
echo "  git add ."
echo "  git commit -m 'sync: update from monorepo'"
echo "  git push origin main"
echo ""
echo "On AL2023 dev box:"
echo "  git pull"
echo "  docker compose up -d --build"
echo ""
