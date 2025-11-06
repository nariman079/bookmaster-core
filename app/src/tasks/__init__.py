"""
Загрузка всех асинхронных задач
"""

from src.tasks.notification_tasks import (
    send_message_telegram_on_master,
    send_message_on_moderator_about_organization,
    send_is_verified_organization,
    send_message_about_verify_master,
    send_message_about_verify_customer,
)
from src.tasks.order_tasks import change_status_order
from src.tasks.csm_metrics_tasks import update_metrics
from src.tasks.kafka_tasks import send_message_in_broker

__all__ = (
    "send_message_telegram_on_master",
    "change_status_order",
    "send_message_on_moderator_about_organization",
    "send_message_about_verify_master",
    "send_message_about_verify_customer",
    "send_is_verified_organization",
    "update_metrics",
    "send_message_in_broker",
)
