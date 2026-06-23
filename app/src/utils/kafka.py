import asyncio
from contextlib import asynccontextmanager
import json
import os
import logging
from typing import Any, Dict, Optional

from django.conf import settings

import aio_pika
from aio_pika import Message, connect_robust


logger = logging.getLogger(__name__)


class DjangoRabbitMQProducer:
    """
    Асинхронный RabbitMQ Producer для Django.
    Предназначен для отправки сообщений в RabbitMQ из Django приложения.
    """

    def __init__(self, amqp_url: str | None = None):
        """
        Инициализация Producer.
        :param amqp_url: Строка подключения к RabbitMQ (например, 'amqp://guest:guest@localhost:5672/').
                         Если не указана, собирается из переменных окружения.
        """
        if amqp_url:
            self.amqp_url = amqp_url
        else:
            # Сборка URL из переменных окружения (аналогично Kafka)
            RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
            RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
            RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
            RABBITMQ_PORT = os.getenv("RABBITMQ_PORT", "5672")
            RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
            
            self.amqp_url = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/{RABBITMQ_VHOST}"
        
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None
        self._started = False

    async def start(self):
        """Запуск RabbitMQ Producer."""
        if not self._started:
            try:
                # Устанавливаем соединение
                self.connection = await connect_robust(self.amqp_url)
                # Создаем канал
                self.channel = await self.connection.channel()
                # Включает подтверждение доставки (persistent mode)
                await self.channel.set_qos(prefetch_count=1)
                
                self._started = True
                logger.info("RabbitMQ producer started.")
            except Exception as e:
                logger.error(f"Failed to start RabbitMQ producer: {e}", exc_info=True)
                raise

    async def stop(self):
        """Остановка RabbitMQ Producer."""
        if self._started:
            if self.channel and not self.channel.is_closed:
                await self.channel.close()
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
            self._started = False
            logger.info("RabbitMQ producer stopped.")

    async def send_message(
        self, 
        queue_name: str, 
        message_data: Dict[str, Any], 
        routing_key: str = None,
        exchange_name: str = "",
        persistent: bool = True,
        priority: int = 0,
        expiration: int = None,
        correlation_id: str = None,
        reply_to: str = None
    ):
        """
        Отправка сообщения в указанную очередь RabbitMQ.
        
        :param queue_name: Название очереди RabbitMQ (если exchange_name не указан, используется прямой обмен)
        :param message_data: Данные сообщения (словарь, который будет сериализован в JSON)
        :param routing_key: Ключ маршрутизации (если не указан, используется queue_name)
        :param exchange_name: Название обмена (по умолчанию '' - default exchange)
        :param persistent: Делать сообщение persistent (сохранять на диск)
        :param priority: Приоритет сообщения (0-9)
        :param expiration: Время жизни сообщения в миллисекундах
        :param correlation_id: ID для корреляции запросов-ответов
        :param reply_to: Очередь для ответов
        """
        if not self._started or not self.channel:
            raise RuntimeError("Producer is not started. Call start() first.")

        try:
            # Получаем обмен
            exchange = await self.channel.get_exchange(exchange_name) if exchange_name else self.channel.default_exchange
            
            # Определяем routing key
            actual_routing_key = routing_key if routing_key else queue_name
            
            # Сериализуем сообщение в JSON
            body = json.dumps(message_data, ensure_ascii=False).encode("utf-8")
            
            # Создаем сообщение
            message = Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT if persistent else aio_pika.DeliveryMode.NOT_PERSISTENT,
                priority=priority,
                correlation_id=correlation_id,
                reply_to=reply_to,
                expiration=expiration
            )
            
            # Отправляем сообщение
            await exchange.publish(
                message,
                routing_key=actual_routing_key
            )
            
            logger.info(
                f"Message sent to exchange '{exchange_name}', routing_key '{actual_routing_key}', queue '{queue_name}'"
            )
        except Exception as e:
            logger.error(f"Failed to send message to queue {queue_name}: {e}", exc_info=True)
            raise

    # Метод для отправки сообщения без ожидания подтверждения
    def send_message_no_wait(
        self,
        queue_name: str,
        message_data: Dict[str, Any],
        routing_key: str = None,
        exchange_name: str = "",
        persistent: bool = True,
        priority: int = 0,
        expiration: int = None,
        correlation_id: str = None,
        reply_to: str = None
    ):
        """
        Отправка сообщения в указанную очередь RabbitMQ без ожидания подтверждения.
        """
        if not self._started or not self.channel:
            raise RuntimeError("Producer is not started. Call start() first.")

        # Используем fire-and-forget подход
        task = asyncio.create_task(
            self._send_and_log(
                queue_name, 
                message_data, 
                routing_key, 
                exchange_name, 
                persistent, 
                priority, 
                expiration, 
                correlation_id, 
                reply_to
            )
        )
        return task

    async def _send_and_log(
        self,
        queue_name: str,
        message_data: Dict[str, Any],
        routing_key: str = None,
        exchange_name: str = "",
        persistent: bool = True,
        priority: int = 0,
        expiration: int = None,
        correlation_id: str = None,
        reply_to: str = None
    ):
        """Внутренний метод для отправки и логирования результата."""
        try:
            exchange = await self.channel.get_exchange(exchange_name) if exchange_name else self.channel.default_exchange
            actual_routing_key = routing_key if routing_key else queue_name
            
            body = json.dumps(message_data, ensure_ascii=False).encode("utf-8")
            
            message = Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT if persistent else aio_pika.DeliveryMode.NOT_PERSISTENT,
                priority=priority,
                correlation_id=correlation_id,
                reply_to=reply_to,
                expiration=expiration
            )
            
            await exchange.publish(message, routing_key=actual_routing_key)
            
            logger.info(
                f"Message (no wait) sent to exchange '{exchange_name}', routing_key '{actual_routing_key}', queue '{queue_name}'"
            )
        except Exception as e:
            logger.error(
                f"Failed to send message (no wait) to queue {queue_name}: {e}", exc_info=True
            )

    # Дополнительный метод для отправки сообщения в конкретную очередь с гарантированной доставкой
    async def send_message_with_confirmation(
        self,
        queue_name: str,
        message_data: Dict[str, Any],
        routing_key: str = None,
        exchange_name: str = "",
        persistent: bool = True,
        timeout: float = 30.0
    ):
        """
        Отправка сообщения с подтверждением доставки (Publisher Confirms).
        
        :param queue_name: Название очереди
        :param message_data: Данные сообщения
        :param routing_key: Ключ маршрутизации
        :param exchange_name: Название обмена
        :param persistent: Persistent режим
        :param timeout: Таймаут ожидания подтверждения
        """
        if not self._started or not self.channel:
            raise RuntimeError("Producer is not started. Call start() first.")

        try:
            # Включаем режим подтверждений на канале
            await self.channel.confirm_select()
            
            exchange = await self.channel.get_exchange(exchange_name) if exchange_name else self.channel.default_exchange
            actual_routing_key = routing_key if routing_key else queue_name
            
            body = json.dumps(message_data, ensure_ascii=False).encode("utf-8")
            
            message = Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT if persistent else aio_pika.DeliveryMode.NOT_PERSISTENT
            )
            
            # Отправляем с ожиданием подтверждения
            await exchange.publish(message, routing_key=actual_routing_key, timeout=timeout)
            
            logger.info(
                f"Message confirmed sent to exchange '{exchange_name}', routing_key '{actual_routing_key}', queue '{queue_name}'"
            )
        except asyncio.TimeoutError:
            logger.error(f"Confirmation timeout for message to queue {queue_name}")
            raise
        except Exception as e:
            logger.error(f"Failed to send confirmed message to queue {queue_name}: {e}", exc_info=True)
            raise

# rabbitmq_config.py


class RabbitMQManager:
    """Менеджер для управления RabbitMQ подключениями в Django"""
    
    _producer: Optional[DjangoRabbitMQProducer] = None
    _initialized = False
    
    @classmethod
    async def get_producer(cls) -> DjangoRabbitMQProducer:
        """Получить или создать экземпляр продюсера"""
        if not cls._producer:
            # Используем настройки из Django settings или переменные окружения
            amqp_url = getattr(settings, 'RABBITMQ_URL', None)
            cls._producer = DjangoRabbitMQProducer(amqp_url=amqp_url)
            await cls._producer.start()
            cls._initialized = True
        return cls._producer
    
    @classmethod
    async def close(cls):
        """Закрыть соединение"""
        if cls._producer and cls._initialized:
            await cls._producer.stop()
            cls._producer = None
            cls._initialized = False

# Контекстный менеджер для использования в Django views
@asynccontextmanager
async def rabbitmq_connection():
    """Контекстный менеджер для работы с RabbitMQ"""
    producer = await RabbitMQManager.get_producer()
    try:
        yield producer
    finally:
        # Не закрываем сразу, т.к. соединение может быть переиспользовано
        pass

