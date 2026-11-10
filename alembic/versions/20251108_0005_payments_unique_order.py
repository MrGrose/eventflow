from collections.abc import Sequence

from alembic import op

revision: str = "20251108_0005"
down_revision: str | Sequence[str] | None = "20251108_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ux_payments_order_id",
        "payments",
        ["order_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ux_payments_order_id", table_name="payments")
