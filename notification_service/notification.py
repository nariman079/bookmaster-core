import os
import asyncio
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Type

from dotenv import load_dotenv
import aio_pika
from aio_pika import IncomingMessage, connect_robust, ExchangeType

from notification_service.events.order_events import Event
from notification_service import notification_handlers as services

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RabbitMQConsumer:
    """Класс для запуска и управления асинхронным консьюмером RabbitMQ."""

    def __init__(self, amqp_url: str, queue_name: str):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.queue = None
        self._running = False

    async def start(self):
        """Запуск RabbitMQ Consumer."""
        try:
            self.connection = await connect_robust(self.amqp_url)
            self.channel = await self.connection.channel()

            # Объявляем очередь, если она не существует
            self.queue = await self.channel.declare_queue(self.queue_name, durable=True)

            # Настраиваем обработчик сообщений
            await self.queue.consume(self._on_message, no_ack=False)

            self._running = True
            logger.info("RabbitMQ consumer started.")

            # Запускаем цикл обработки сообщений
            await asyncio.Future()  # run forever until cancelled

        except Exception as e:
            logger.error(f"Failed to start RabbitMQ consumer: {e}", exc_info=True)
            raise

    async def stop(self):
        """Остановка RabbitMQ Consumer."""
        if self._running:
            self._running = False
            if self.channel and not self.channel.is_closed:
                await self.channel.close()
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
            logger.info("RabbitMQ consumer stopped.")

    async def _on_message(self, message: IncomingMessage):
        """Обработчик входящих сообщений."""
        try:
            async with message.process():
                message_data = json.loads(message.body.decode("utf-8"))
                event_type = message_data.get("event")
                metadata = message_data.get("metadata")
                object_data = message_data.get("data")
                notification_type = message_data.get("notification_type")

                event = Event(
                    data=object_data,
                    event_type=event_type,
                    metadata=metadata,
                )

                extra_data = {
                    "delivery_tag": message.delivery_tag,
                    **metadata,
                }

                await self._handle_notification_async(event, extra_data)

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    async def _handle_notification_async(self, event: Event, extra: dict):
        """Асинхронная функция обработчика уведомления с логированием."""
        try:
            handlers = services.get_event_handlers()
            handler = handlers.get(event.event_type)
            if handler:
                await handler(event, extra)
            else:
                logger.warning(f"No handler found for notification type: {event.event_type}")
        except Exception as e:
            logger.error(f"Failed to process notification: {e}", exc_info=True)

async def main():
    """Точка входа для запуска консьюмера."""
    amqp_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    queue_name = "default_queue"

    consumer = RabbitMQConsumer(amqp_url, queue_name)

    try:
        await consumer.start()
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    load_dotenv()
    asyncio.run(main())