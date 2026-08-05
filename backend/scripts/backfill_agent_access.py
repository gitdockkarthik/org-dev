#!/usr/bin/env python3
"""Backfill agent_access table with all (user, published-agent) pairs.

Chunk 2 of Agent-level access control rollout: populate agent_access so no user
sees a blank catalogue on first deploy. Access is then manually curated per-user.

Exclusion: mock-agent is only granted to karthikeyan.gopalan@operative.com,
all other users are skipped for this agent.

To run: docker compose exec backend python scripts/backfill_agent_access.py
"""
import asyncio
import sys
from datetime import datetime, timezone

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import AsyncSessionLocal
from models.agent import Agent, AgentStatus
from models.user import User
from models.agent_access import AgentAccess


async def backfill_agent_access():
    """Backfill agent_access table for all users x published agents."""
    async with AsyncSessionLocal() as session:
        # Fetch all published agents
        agent_result = await session.execute(
            select(Agent).where(Agent.status == AgentStatus.published)
        )
        published_agents = agent_result.scalars().all()

        # Fetch all users
        user_result = await session.execute(select(User))
        all_users = user_result.scalars().all()

        total_agents = len(published_agents)
        total_users = len(all_users)

        print(f"Total published agents: {total_agents}")
        print(f"Total users: {total_users}")
        print()

        rows_inserted = 0
        rows_skipped = 0
        mock_agent_excluded_count = 0

        # Iterate over all (user, agent) pairs
        for user in all_users:
            for agent in published_agents:
                # Exclusion: mock-agent only for karthikeyan.gopalan@operative.com
                if agent.slug == 'mock-agent' and user.email != 'karthikeyan.gopalan@operative.com':
                    rows_skipped += 1
                    mock_agent_excluded_count += 1
                    continue

                # Check if (agent_slug, user_email) pair already exists
                existing = await session.execute(
                    select(AgentAccess).where(
                        and_(
                            AgentAccess.agent_slug == agent.slug,
                            AgentAccess.user_email == user.email
                        )
                    )
                )

                if existing.scalars().first() is None:
                    # Insert new access record
                    access = AgentAccess(
                        agent_slug=agent.slug,
                        user_email=user.email,
                        assigned_by='system-backfill',
                        assigned_at=datetime.now(timezone.utc),
                    )
                    session.add(access)
                    rows_inserted += 1

        # Commit all inserts
        await session.commit()

        # Print summary
        print("=" * 70)
        print("BACKFILL SUMMARY")
        print("=" * 70)
        print(f"Total users:              {total_users}")
        print(f"Total published agents:   {total_agents}")
        print(f"Rows inserted:            {rows_inserted}")
        print(f"Rows skipped:             {rows_skipped}")
        print()
        print(f"Excluded agent_slug=mock-agent for {mock_agent_excluded_count} users")
        print(f"  (all except karthikeyan.gopalan@operative.com)")
        print("=" * 70)


if __name__ == '__main__':
    try:
        asyncio.run(backfill_agent_access())
        print("\n✓ Backfill completed successfully")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Backfill failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
