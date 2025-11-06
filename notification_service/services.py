from aiogram import Bot
from notification_service.utils import RequestLogger  

master_bot = Bot(token="8502011901:AAEKrM1xQ2TRW-k1nuTOpEdczw4QPwN-tPk")
organization_bot = Bot(token="8502011901:AAEKrM1xQ2TRW-k1nuTOpEdczw4QPwN-tPk")

class CreateOrderNotificationService:
    def __init__(self, data: dict[str, any]):
        self.data = data

    async def _send_notification_on_master(self):
        """Отправка сообщения о новой брони мастеру"""
        request_id = self.data["request_id"]
        logger = RequestLogger(request_id)

        master_telegram_id = self.data["master_telegram_id"]
        client_phone_number = self.data["customer_phone"]
        booking_date = self.data["booking_date"]
        booking_time = self.data["booking_time"]

        logger.info(
          "Началась отправка телеграм сообщения",
            extra={
                "master_telegram_id": master_telegram_id,
               "event": "notify.send.telegram",
            },
        )

        # text = await generate_message_text()

        text = (
            f"У вас новая бронь\n"
            f"Клиент: {client_phone_number}\n"
            f"Дата: {booking_date}\n"
            f"Время: {booking_time}"
        )

        try:
            await master_bot.send_message(chat_id=master_telegram_id, text=text)
            logger.debug(
                "Сообщение о бронировании успешно отправлено",
                extra={
                    "master_telegram_id": master_telegram_id,
                    "event": "notify.send.telegram",
                },
            )
        except Exception as error:
            logger.error(
                "Ошибка отправки телеграм сообщения, пробуем снова",
                extra={
                    "error_message": str(error),
                    "master_telegram_id": master_telegram_id,
                    "event": "notify.send.telegram",
                },
            )
            raise  

    async def execute(self):
        await self._send_notification_on_master()


class VerifyMasterNotificationService:
    def __init__(self, data: dict[str, any]): 
        self.data = data
    
    async def _send_message_about_verified_master(self):
        """Отправка сообщения об успешной верификации мастера"""
        self.master_name = self.data['master_name']
        self.master_surname = self.data['master_surname']
        self.organization_telegram_id = self.data['organization_telegram_id']

        text = f"✅ Мастер {self.master_name} {self.master_surname} зарегистрировался в системе \n"
    
        await organization_bot.send_message(
            chat_id=self.organization_telegram_id,
            text=text
        )
        
    async def execute(self):
        await self._send_message_about_verified_master()