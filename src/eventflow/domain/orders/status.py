from enum import StrEnum


class OrderStatus(StrEnum):
    DRAFT = "draft"
    CONFIRMED = "confirmed"
    CANCELLED = "cancelled"
    PAID = "paid"
    PAYMENT_FAILED = "payment_failed"
