"""Add zabbix_alert_detail -- one row per Zabbix alert with what OpsGenie keeps in the
alert's description and not in its title: Zabbix's own problem ID (the same on an Open
notice and on the Closed notice that recovers it), the full problem name, the host and the
severity. OpsGenie cuts an alert's message at 130 characters, so about a third of Zabbix
titles lose the end of the check text (the threshold), and matching Open and Closed notices
by title paired the wrong notices (141 of 317 prefix matches and about 3% of exact matches
checked on 2026-10-07). Matching on problem_id is exact.
Filled by the enrichment job from Get Alert. fetch_status records why a row has no
problem_id (ok, no_description, not_found, error) so that only errors are retried.
The agent runs Base.metadata.create_all() at startup, so if the new model is deployed before
this revision is applied the table already exists; that case is tolerated (nothing is
dropped or changed), so the order of the two cannot break either. downgrade() is tolerant in
the same way: it drops only what exists.

Revision ID: 0057
Revises: 0056
Create Date: 2026-10-07
"""
from alembic import op
import sqlalchemy as sa


revision = "0057"
down_revision = "0056"
branch_labels = None
depends_on = None

TABLE = "zabbix_alert_detail"
INDEX = "ix_zabbix_alert_detail_problem_id"


def upgrade() -> None:
    if not sa.inspect(op.get_bind()).has_table(TABLE):
        op.create_table(
            TABLE,
            sa.Column("alert_id", sa.Text, primary_key=True),
            sa.Column("problem_id", sa.Text, nullable=True),
            sa.Column("problem_name", sa.Text, nullable=True),
            sa.Column("full_message", sa.Text, nullable=True),
            sa.Column("title_status", sa.Text, nullable=True),
            sa.Column("kind", sa.Text, nullable=True),
            sa.Column("host", sa.Text, nullable=True),
            sa.Column("severity", sa.Text, nullable=True),
            sa.Column("fetch_status", sa.Text, nullable=False),
            sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        )
    if INDEX not in [i["name"] for i in sa.inspect(op.get_bind()).get_indexes(TABLE)]:
        op.create_index(INDEX, TABLE, ["problem_id"])


def downgrade() -> None:
    insp = sa.inspect(op.get_bind())
    if insp.has_table(TABLE):
        if INDEX in [i["name"] for i in insp.get_indexes(TABLE)]:
            op.drop_index(INDEX, table_name=TABLE)
        op.drop_table(TABLE)
