from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20251108_0003"
down_revision: str | Sequence[str] | None = "20251108_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "outbox_events", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        "outbox_events", sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        "outbox_events",
        sa.Column(
            "broker_meta",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.create_index("ix_outbox_events_next_attempt", "outbox_events", ["next_attempt_at"])


def downgrade() -> None:
    op.drop_index("ix_outbox_events_next_attempt", table_name="outbox_events")
    op.drop_column("outbox_events", "broker_meta")
    op.drop_column("outbox_events", "next_attempt_at")
    op.drop_column("outbox_events", "lease_expires_at")
