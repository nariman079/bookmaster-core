from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any
import asyncio
import datetime
from typing import Dict, Callable, Any
import logging

from notification_service.events.order_events import Event
from notification_service.services import (
    CreateOrderNotificationService,
    VerifyMasterNotificationService,
)

logger = logging.getLogger(__name__)

# Глобальный реестр: event_name → async функция
_EVENT_HANDLERS: Dict[str, Callable] = {}


def get_event_handlers() -> dict[str, callable]:
    """Возвращает копию реестра обработчиков."""
    return _EVENT_HANDLERS.copy()


def event_handler(event_name: str):
    """
    Декоратор для регистрации асинхронного обработчика события.

    Использование:
        @event_handler('order.created')
        async def handle_order_created(event_data: dict, metadata: dict):
            ...
    """

    def decorator(func: Callable) -> Callable:
        if not asyncio.iscoroutinefunction(func):
            raise ValueError(f"Handler {func.__name__} must be async")

        if event_name in _EVENT_HANDLERS:
            logger.warning(
                f"Handler for event '{event_name}' is already registered. Overriding."
            )

        _EVENT_HANDLERS[event_name] = func
        logger.info(
            f"Registered event handler '{func.__name__}' for event '{event_name}'"
        )
        return func

    return decorator


@event_handler("order.create")
async def create_order_handler(event: Event, extra: dict):
    event.data["request_id"] = extra.get("request_id")
    create_order_srv = CreateOrderNotificationService(event.data)
    await create_order_srv.execute()


@event_handler("master.verify")
async def verify_master(event: Event, extra: dict):
    event.data["request_id"] = extra.get("request_id")
    verify_master_srv = VerifyMasterNotificationService(event.data)
    await verify_master_srv.execute()
