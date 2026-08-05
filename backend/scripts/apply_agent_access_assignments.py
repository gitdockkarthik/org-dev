#!/usr/bin/env python3
"""Apply curated agent access assignments to specific users.

Standalone async script (not a migration) that applies exact agent_slug assignments
to named users. Does not touch users not in the ASSIGNMENTS dict.

Protected users (never modified):
  - karthikeyan.gopalan@operative.com
  - test.user@operative.com
  - test.dev@operative.com

To run: docker compose exec backend python scripts/apply_agent_access_assignments.py
"""
import asyncio
import sys
from datetime import datetime, timezone

from sqlalchemy import select, and_, delete
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import AsyncSessionLocal
from models.agent_access import AgentAccess


# Curated agent access assignments: user_email -> list of agent_slugs
ASSIGNMENTS = {
    "ajith.nair@operative.com": ["alert-analyser", "cur-analyser", "kafka-analyser", "rca-agent", "app-support-v2"],
    "praveen.kd@operative.com": ["alert-analyser", "cur-analyser", "kafka-analyser", "rca-agent", "app-support-v2"],
    "sridharan.v@operative.com": ["alert-analyser", "cur-analyser", "kafka-analyser", "rca-agent", "app-support-v2"],
    "sonam.pawar@operative.com": ["alert-analyser"],
    "amritanshu.bhardwaj@operative.com": ["alert-analyser", "cur-analyser", "kafka-analyser", "rca-agent", "app-support-v2"],
    "ajikuttan.balan@operative.com": ["cur-analyser"],
    "lokeshkumar.pt@operative.com": ["alert-analyser", "kafka-analyser"],
    "dhanya.naik@operative.com": ["alert-analyser"],
    "balasubramanian.p@operative.com": ["alert-analyser", "rca-agent", "kafka-analyser"],
    "vignesh.c@operative.com": ["alert-analyser", "rca-agent"],
    "harshith.gs@operative.com": ["alert-analyser"],
    "akshay.sk@operative.com": ["alert-analyser"],
    "puja.savadi@operative.com": ["alert-analyser"],
    "tarun.konda@operative.com": ["alert-analyser"],
    "vidhya.p@operative.com": ["alert-analyser"],
    "abhilasha.r@operative.com": ["alert-analyser"],
    "ruthu.m@operative.com": ["alert-analyser"],
    "akshay.a@operative.com": ["alert-analyser"],
    "shivam.saroj@operative.com": ["alert-analyser", "rca-agent"],
    "praveen.alluru@operative.com": ["alert-analyser", "cur-analyser", "kafka-analyser", "rca-agent", "app-support-v2"],
    "amit.chougule@operative.com": ["kafka-analyser"],
    "rakesh.thakur@operative.com": ["alert-analyser", "kafka-analyser"],
    "nisar.pp@operative.com": ["alert-analyser", "kafka-analyser"],
    "somnath.b@operative.com": ["alert-analyser", "kafka-analyser"],
    "abeshek.a@operative.com": ["alert-analyser", "kafka-analyser"],
    "syed.muddassir@operative.com": ["alert-analyser", "kafka-analyser"],
    "venkata.pn@operative.com": ["alert-analyser", "kafka-analyser"],
    "narayanag@operative.com": ["alert-analyser", "kafka-analyser"],
}

# Protected users — never modified
PROTECTED_USERS = {
    "karthikeyan.gopalan@operative.com",
    "test.user@operative.com",
    "test.dev@operative.com",
}


async def apply_agent_access_assignments():
    """Apply curated agent access assignments."""
    async with AsyncSessionLocal() as session:
        total_users_updated = 0
        summary_lines = []

        for user_email, agent_slugs in ASSIGNMENTS.items():
            # Skip protected users (should not happen, but safety check)
            if user_email in PROTECTED_USERS:
                print(f"⚠ Skipping protected user: {user_email}")
                continue

            # Delete all existing AgentAccess rows for this user
            await session.execute(
                delete(AgentAccess).where(
                    AgentAccess.user_email == user_email
                )
            )

            # Insert new AgentAccess rows for each assigned slug
            for slug in agent_slugs:
                access = AgentAccess(
                    agent_slug=slug,
                    user_email=user_email,
                    assigned_by='system-bulk-update',
                    assigned_at=datetime.now(timezone.utc),
                )
                session.add(access)

            total_users_updated += 1
            summary_lines.append(f"  {user_email:<40} {', '.join(agent_slugs)}")

        # Single commit at the end
        await session.commit()

        # Print summary
        print("=" * 100)
        print("AGENT ACCESS ASSIGNMENT SUMMARY")
        print("=" * 100)
        print()
        print("User Email                                   Assigned Agent Slugs")
        print("-" * 100)
        for line in summary_lines:
            print(line)
        print()
        print("=" * 100)
        print(f"Total users updated: {total_users_updated}")
        print(f"Protected users (untouched): {len(PROTECTED_USERS)}")
        print("=" * 100)


if __name__ == '__main__':
    try:
        asyncio.run(apply_agent_access_assignments())
        print("\n✓ Agent access assignments applied successfully")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Assignment failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
