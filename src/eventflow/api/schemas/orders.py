from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field


class OrderItemIn(BaseModel):
    sku: str = Field(min_length=1, max_length=64)
    quantity: int = Field(gt=0)
    unit_price: Decimal = Field(gt=0)


class CreateOrderRequest(BaseModel):
    customer_id: str = Field(min_length=1, max_length=128)
    items: list[OrderItemIn] = Field(min_length=1)


class OrderItemOut(BaseModel):
    sku: str
    quantity: int
    unit_price: Decimal


class OrderOut(BaseModel):
    id: UUID
    customer_id: str
    status: str
    items: list[OrderItemOut]
    total: Decimal
