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


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



@dataclass
class Order:
    """Датакласс для уведомления."""
    order_id: int
    master_id: str
    customer_phone: str
    begin_date: str
    begin_time: str
    request_id: str

@dataclass
class Notify:
    event: str
    obj: dict
    _from: str
    metadata: dict
    notification_type: str
    

class NotificationHandler(ABC):
    """Абстрактный базовый класс для обработчиков уведомлений."""

    @abstractmethod
    async def send(self, notify: Notify, extra: dict):
        """Абстрактный метод для отправки уведомления."""
        pass


class TelegramNotificationHandler(NotificationHandler):
    """Обработчик для отправки уведомлений в Telegram."""

    async def send(self, notify: Notify, extra: dict):
        # Заглушка для отправки в Telegram
        logger.info(f"Sending Telegram notification to {notify.recipient}: {notify.message}")
        # await some_async_telegram_library.send_message(...)
        await asyncio.sleep(0.1)  # Имитация асинхронной операции


class EmailNotificationHandler(NotificationHandler):
    """Обработчик для отправки уведомлений по Email."""

    async def send(self, notify: Notify, extra: dict):
        # Заглушка для отправки по Email
        logger.info(f"Sending Email notification to {notify.recipient}: {notify.message}")
        # await some_async_email_library.send(...)
        await asyncio.sleep(0.1)  # Имитация асинхронной операции


class SMSNotificationHandler(NotificationHandler):
    """Обработчик для отправки SMS уведомлений."""

    async def send(self, notify: Notify, extra: dict):
        # Заглушка для отправки SMS
        logger.info(f"Sending SMS notification to {notify.recipient}: {notify.message}")
        # await some_async_sms_library.send(...)
        await asyncio.sleep(0.1)  


class NotificationConsumer:
    """Класс для запуска и управления асинхронным консьюмером Kafka."""

    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = None
        self.handlers: Dict[str, NotificationHandler] = {}
        self._running = False

    def register_handler(self, notification_type: str, handler: NotificationHandler):
        """Регистрация обработчика для конкретного типа уведомления."""
        self.handlers[notification_type] = handler
        logger.info(f"Handler for notification type '{notification_type}' registered.")

    async def start(self):
        """Запуск Kafka Consumer."""
        KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
        KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD') 
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda m: json.loads(v.decode('utf-8')),  
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
                logger.info(f"Processing event '{event}' from {_from}")                
                event = message_data.get('event')
                metadata = message_data.get('metadata')
                object_data = message_data.get('data')
                _from = message_data.get('_from')
                notification_type = message_data.get('notification_type')
                notify_obj = Notify(
                    obj=object_data,
                    event=event,
                    _from=_from,
                    metadata=metadata,
                    notification_type=notification_type
                )
                extra_data = {
                    "kafka_offset": msg.offset, 
                    "partition": msg.partition,
                    **metadata
                }
                await self._handle_notification_async(notify_obj, extra_data)

            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

    async def _handle_notification_async(self, notify: Notify, extra: dict):
        """Асинхронная функция обработчика уведомления с логированием."""
        logger.info(f"Handling notification for user {notify.user_id}, type: {notify.notification_type}, recipient: {notify.recipient}")
        try:
            handler = self.handlers.get(notify.notification_type)
            if handler:
                await handler.send(notify, extra)
                logger.info(f"Notification of type {notify.notification_type} processed successfully for user {notify.user_id}.")
            else:
                logger.warning(f"No handler found for notification type: {notify.notification_type}")
        except Exception as e:
            logger.error(f"Failed to process notification: {e}", exc_info=True)


async def main():
    """Точка входа для запуска консьюмера."""
    consumer = NotificationConsumer(
        bootstrap_servers=os.getenv('KAFKA_HOST'),
        topic='default_topic'
    )

    consumer.register_handler('telegram', TelegramNotificationHandler())
    consumer.register_handler('email', EmailNotificationHandler())
    consumer.register_handler('sms', SMSNotificationHandler())

    try:
        await consumer.start()
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    load_dotenv()

    asyncio.run(main())