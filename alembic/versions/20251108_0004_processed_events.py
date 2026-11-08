from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20251108_0004"
down_revision: str | Sequence[str] | None = "20251108_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "processed_events",
        sa.Column("consumer_name", sa.String(length=64), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("consumer_name", "event_id"),
    )


def downgrade() -> None:
    op.drop_table("processed_events")
