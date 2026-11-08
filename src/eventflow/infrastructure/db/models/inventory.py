from sqlalchemy import Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from eventflow.infrastructure.db.base import Base


class StockItem(Base):
    __tablename__ = "stock_items"
    sku: Mapped[str] = mapped_column(String(64), primary_key=True)
    available: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    __table_args__ = (Index("ix_stock_items_sku", "sku"),)
