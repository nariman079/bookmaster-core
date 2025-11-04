from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class EventMessage:
    event: str
    notification_type: str
    data: Dict[str, Any]
    metadata: Dict[str, Any]

class EventHandler(ABC):
    @abstractmethod
    async def handle(self, message: EventMessage, extra: dict):
        pass


class MasterCreatedHandler(EventHandler):
    async def handle(self, message: EventMessage, extra: dict):
        logger.info("Handling master creation")
        # Логика уведомления админа
        await self.send_email(...)

class OrderCreatedHandler(EventHandler):
    async def handle(self, message: EventMessage, extra: dict):
        logger.info("Handling order creation")
        # Твоя текущая логика

