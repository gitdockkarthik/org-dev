#!/bin/bash
set -e

# ═══════════════════════════════════════════════
# Operative Dev Platform — Environment Setup
# Run once before first docker compose up
# ═══════════════════════════════════════════════

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo ""
echo "═══════════════════════════════════════════"
echo "  Operative Dev Platform — Environment Setup"
echo "═══════════════════════════════════════════"
echo ""

# Check if .env already exists
if [ -f .env ]; then
  echo -e "${YELLOW}⚠️  .env already exists.${NC}"
  read -p "   Overwrite it? (y/N): " confirm
  if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "   Aborted. Existing .env kept."
    exit 0
  fi
fi

# Check if postgres volume already exists
if docker volume ls | grep -q "org-dev_pgdata"; then
  echo ""
  echo -e "${RED}⚠️  WARNING: Existing database volume detected.${NC}"
  echo ""
  echo "   Generating a new ENCRYPTION_KEY will NOT match"
  echo "   your existing PostgreSQL data."
  echo ""
  echo "   Options:"
  echo "   1. Reset completely (loses all data):"
  echo "      docker compose down -v"
  echo "      Then rerun this script."
  echo ""
  echo "   2. Keep existing data (recommended):"
  echo "      Do NOT rerun this script."
  echo "      Your existing .env is still valid."
  echo ""
  read -p "   Continue anyway and generate new keys? (y/N): " db_confirm
  if [[ "$db_confirm" != "y" && "$db_confirm" != "Y" ]]; then
    echo "   Aborted. Existing .env and database kept."
    exit 0
  fi
  echo ""
  echo -e "${YELLOW}   Continuing — remember to run:${NC}"
  echo "   docker compose down -v"
  echo "   before docker compose up"
  echo ""
fi

# Check dependencies
if ! command -v python3 &> /dev/null; then
  echo -e "${RED}✗ python3 not found. Please install Python 3.${NC}"
  exit 1
fi
if ! command -v openssl &> /dev/null; then
  echo -e "${RED}✗ openssl not found. Please install openssl.${NC}"
  exit 1
fi

echo "Generating secrets..."
echo ""

# Generate values
ENCRYPTION_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
POSTGRES_PASSWORD=$(openssl rand -hex 16)
SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
BACKEND_API_KEY=$(openssl rand -hex 32)

# Copy .env.example to .env
cp .env.example .env

# Replace placeholders in .env
sed -i.bak "s|ENCRYPTION_KEY=.*|ENCRYPTION_KEY=${ENCRYPTION_KEY}|" .env
sed -i.bak "s|POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=${POSTGRES_PASSWORD}|" .env
sed -i.bak "s|SECRET_KEY=.*|SECRET_KEY=${SECRET_KEY}|" .env
sed -i.bak "s|BACKEND_API_KEY=.*|BACKEND_API_KEY=${BACKEND_API_KEY}|" .env
sed -i.bak "s|DATABASE_URL=.*|DATABASE_URL=postgresql+asyncpg://operative:${POSTGRES_PASSWORD}@postgres:5432/operative_db|" .env

# Remove backup file created by sed
rm -f .env.bak

# Set secure permissions
chmod 600 .env

echo -e "${GREEN}✓ ENCRYPTION_KEY generated${NC}"
echo -e "${GREEN}✓ POSTGRES_PASSWORD generated${NC}"
echo -e "${GREEN}✓ SECRET_KEY generated${NC}"
echo -e "${GREEN}✓ BACKEND_API_KEY generated${NC}"
echo -e "${GREEN}✓ DATABASE_URL configured${NC}"
echo -e "${GREEN}✓ .env created with secure permissions (600)${NC}"
echo ""
echo "═══════════════════════════════════════════"
echo -e "${RED}⚠️  CRITICAL — BACK UP YOUR ENCRYPTION KEY${NC}"
echo "═══════════════════════════════════════════"
echo ""
echo "Your ENCRYPTION_KEY:"
echo ""
echo -e "  ${YELLOW}${ENCRYPTION_KEY}${NC}"
echo ""
echo "This key encrypts ALL secrets in PostgreSQL."
echo "If lost, encrypted data cannot be recovered."
echo ""
echo "Back it up NOW to a secure location:"
echo "  • Password manager"
echo "  • AWS Secrets Manager"
echo "  • Secure note"
echo ""
echo "═══════════════════════════════════════════"
echo "  Next Steps"
echo "═══════════════════════════════════════════"
echo ""
echo "1. Back up the ENCRYPTION_KEY above"
echo ""
echo "2. Start the platform:"
echo "   docker compose up -d postgres backend portal"
echo ""
echo "3. Open in browser:"
echo "   http://localhost:3000"
echo ""
echo "4. Complete Setup Wizard:"
echo "   • Enter Anthropic API key"
echo "   • Copy and save the generated Backend API key"
echo ""
echo "5. Start agents:"
echo "   docker compose up -d alert-analyser cur-analyser infra-health"
echo ""
echo "6. Configure each agent via Settings UI:"
echo "   • Select data source"
echo "   • Click Save & Sync"
echo ""
echo "═══════════════════════════════════════════"
echo -e "${GREEN}✓ Setup complete. Ready to start.${NC}"
echo "═══════════════════════════════════════════"
echo ""
