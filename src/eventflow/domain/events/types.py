from enum import StrEnum


class EventType(StrEnum):
    ORDER_CREATED = "order.created"
    ORDER_CONFIRMED = "order.confirmed"
    ORDER_CANCELLED = "order.cancelled"
    PAYMENT_REQUESTED = "payment.requested"
    PAYMENT_COMPLETED = "payment.completed"
    PAYMENT_FAILED = "payment.failed"
    INVENTORY_RESERVED = "inventory.reserved"
    INVENTORY_RESERVATION_FAILED = "inventory.reservation_failed"
