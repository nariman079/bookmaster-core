import os
import asyncio
from celery import shared_task

from src.utils.kafka import DjangoKafkaProducer

kafka_host = os.getenv('KAFKA_HOST')

@shared_task
def send_message_in_broker(message_data: dict):
    """Отправка сообщения в Kafka из Celery (синхронный таск с асинхронной логикой)"""
    async def _send():
        producer = DjangoKafkaProducer(bootstrap_servers=kafka_host)
        try:
            await producer.start()
            print(message_data)
            await producer.send_message(
                topic='default_topic',
                message_data=message_data,
                key=message_data.get('order_id')
            )
        finally:
            await producer.stop()

    asyncio.run(_send())