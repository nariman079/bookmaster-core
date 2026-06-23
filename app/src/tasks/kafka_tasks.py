import asyncio
from celery import shared_task

from src.utils.kafka import DjangoRabbitMQProducer



@shared_task
def send_message_in_broker(message_data: dict, topic: str = "default_topic"):
    """Отправка сообщения в RabbitMQ из Celery (синхронный таск с асинхронной логикой)"""

    async def _send():
        producer = DjangoRabbitMQProducer()
        try:
            await producer.start()
            print(message_data)
            await producer.send_message(
                queue_name=topic, message_data=message_data
            )
        finally:
            await producer.stop()

    asyncio.run(_send())
