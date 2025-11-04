import json
import logging
from typing import Any, Dict
from aiokafka import AIOKafkaProducer
import asyncio

# Настройка логирования
logger = logging.getLogger(__name__)


class DjangoKafkaProducer:
    """
    Асинхронный Kafka Producer для Django.
    Предназначен для отправки сообщений в Kafka из Django приложения.
    """

    def __init__(self, bootstrap_servers: str):
        """
        Инициализация Producer.
        :param bootstrap_servers: Строка подключения к Kafka (например, 'localhost:9092').
        """
        self.bootstrap_servers = bootstrap_servers
        self.producer: AIOKafkaProducer | None = None
        self._started = False

    async def start(self):
        """Запуск Kafka Producer."""
        KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
        KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD') 
        if not self._started:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="SCRAM-SHA-512",
                sasl_plain_username=KAFKA_USERNAME,
                sasl_plain_password=KAFKA_PASSWORD,
            )
            await self.producer.start()
            self._started = True
            logger.info("Kafka producer started.")

    async def stop(self):
        """Остановка Kafka Producer."""
        if self._started and self.producer:
            await self.producer.stop()
            self._started = False
            logger.info("Kafka producer stopped.")

    async def send_message(self, topic: str, message_data: Dict[str, Any], key: str = None):
        """
        Отправка сообщения в указанный топик Kafka.
        :param topic: Название топика Kafka.
        :param message_data: Данные сообщения (словарь, который будет сериализован в JSON).
        :param key: Ключ сообщения (опционально).
        """
        if not self._started or not self.producer:
            raise RuntimeError("Producer is not started. Call start() first.")

        try:
            future = await self.producer.send_and_wait(topic, value=message_data, key=key.encode('utf-8') if key else None)
            logger.info(f"Message sent to topic {topic}, partition {future.partition}, offset {future.offset}")
        except Exception as e:
            logger.error(f"Failed to send message to topic {topic}: {e}", exc_info=True)
            raise # Пробрасываем ошибку, чтобы вызывающий код мог обработать её

    # Опционально: метод для отправки сообщения без ожидания подтверждения
    def send_message_no_wait(self, topic: str, message_data: Dict[str, Any], key: str = None):
        """
        Отправка сообщения в указанный топик Kafka без ожидания подтверждения.
        :param topic: Название топика Kafka.
        :param message_data: Данные сообщения.
        :param key: Ключ сообщения (опционально).
        """
        if not self._started or not self.producer:
            raise RuntimeError("Producer is not started. Call start() first.")

        # Используем fire-and-forget подход
        # Подписываемся на результат внутри метода, логируем ошибки
        task = asyncio.create_task(
            self._send_and_log(topic, message_data, key)
        )
        # Не ждем завершения задачи, но можно сохранить для отслеживания
        # например, в self._pending_tasks для graceful shutdown
        return task

    async def _send_and_log(self, topic: str, message_data: Dict[str, Any], key: str = None):
        """Внутренний метод для отправки и логирования результата."""
        try:
            future = await self.producer.send_and_wait(topic, value=message_data, key=key.encode('utf-8') if key else None)
            logger.info(f"Message (no wait) sent to topic {topic}, partition {future.partition}, offset {future.offset}")
        except Exception as e:
            logger.error(f"Failed to send message (no wait) to topic {topic}: {e}", exc_info=True)
