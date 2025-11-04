import os

from celery import shared_task

from src.utils.kafka import DjangoKafkaProducer

kafka_host = os.getenv('KAFKA_HOST')
kafka_producer = DjangoKafkaProducer(bootstrap_servers=kafka_host)

@shared_task
def send_message_in_broker(
    message_data: dict[any, any],
):
    """Отправка сообщения в топки в kafka"""
    
    kafka_producer.start()
    kafka_producer.send_message('default_topic', message_data=message_data)
    kafka_producer.stop()
    