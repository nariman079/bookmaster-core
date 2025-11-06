import os
import asyncio
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Type

from dotenv import load_dotenv
import aiokafka
from aiokafka import AIOKafkaConsumer

from notification_service.events.order_events import Event
from notification_service import notification_handlers as services

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NotificationConsumer:
    """Класс для запуска и управления асинхронным консьюмером Kafka."""

    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = None
        self._running = False

    async def start(self):
        """Запуск Kafka Consumer."""
        KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
        KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
        )
        await self.consumer.start()
        self._running = True
        logger.info("Kafka consumer started.")
        await self._consume_messages()

    async def stop(self):
        """Остановка Kafka Consumer."""
        self._running = False
        if self.consumer:
            await self.consumer.stop()
        logger.info("Kafka consumer stopped.")

    async def _consume_messages(self):
        """Основной цикл обработки сообщений из Kafka."""
        async for msg in self.consumer:
            try:
                message_data = msg.value
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
                    "kafka_offset": msg.offset,
                    "partition": msg.partition,
                    **metadata,
                }
                # logger.info(f"Processing event '{event}' from {_from}")

                await self._handle_notification_async(event, extra_data)

            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

    async def _handle_notification_async(self, event: Event, extra: dict):
        """Асинхронная функция обработчика уведомления с логированием."""
        # logger.info(f"Handling notification for user {event.event_type.event}, type: {event.notification_type}")
        try:
            handlers = services.get_event_handlers()
            handler = handlers.get(event.event_type)
            if handler:
                await handler(event, extra)
                # logger.info(f"Notification of type {event.notification_type} processed successfully for user ")
            else:
                pass
                # logger.warning(f"No handler found for notification type: {event.notification_type}")
        except Exception as e:
            logger.error(f"Failed to process notification: {e}", exc_info=True)


async def main():
    """Точка входа для запуска консьюмера."""
    consumer = NotificationConsumer(
        bootstrap_servers=os.getenv("KAFKA_HOST"), topic="default_topic"
    )

    try:
        await consumer.start()
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    load_dotenv()

    asyncio.run(main())
