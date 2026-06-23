import asyncio
import logging
from typing import Dict, Callable

from notification_service.events.order_events import Event
from notification_service.services import (
    CreateOrderNotificationService,
    VerifyMasterNotificationService,
    master_bot,
    organization_bot,
    moderator_bot
)

logger = logging.getLogger(__name__)

# Глобальный реестр: event_name → async функция
_EVENT_HANDLERS: Dict[str, Callable] = {}


def get_event_handlers() -> dict[str, callable]:
    """Возвращает копию реестра обработчиков."""
    return _EVENT_HANDLERS.copy()


def event_handler(event_name: str):
    """
    Декоратор для регистрации асинхронного обработчика события.

    Использование:
        @event_handler('order.created')
        async def handle_order_created(event_data: dict, metadata: dict):
            ...
    """

    def decorator(func: Callable) -> Callable:
        if not asyncio.iscoroutinefunction(func):
            raise ValueError(f"Handler {func.__name__} must be async")

        if event_name in _EVENT_HANDLERS:
            logger.warning(
                f"Handler for event '{event_name}' is already registered. Overriding."
            )

        _EVENT_HANDLERS[event_name] = func
        logger.info(
            f"Registered event handler '{func.__name__}' for event '{event_name}'"
        )
        return func

    return decorator


@event_handler("order.create")
async def create_order_handler(event: Event, extra: dict):
    """
    Уведомление мастера о создании брони.
    Полностью заменяет send_message_telegram_on_master.
    Ожидаемые поля в event.data:
      - master_telegram_id
      - customer_phone
      - booking_date
      - booking_time
    """
    event.data["request_id"] = extra.get("request_id")
    create_order_srv = CreateOrderNotificationService(event.data)
    await create_order_srv.execute()


@event_handler("master.verify")
async def verify_master(event: Event, extra: dict):
    """
    Событие: мастер успешно верифицирован.
    Перенос логики send_message_about_verify_master:
      - отправка в организацию сообщения о новом мастере.
    Ожидаемые поля в event.data:
      - master_name
      - master_surname
      - organization_telegram_id
    """
    event.data["request_id"] = extra.get("request_id")
    verify_master_srv = VerifyMasterNotificationService(event.data)
    await verify_master_srv.execute()


# --- События модераторов ---
@event_handler('moderator.telegram_linked')
async def moderator_telegram_linked_handler(event: Event, extra: dict):
    """
    Событие: модератор привязал Telegram.

    Сейчас ограничимся логированием, чтобы подтвердить факт события.
    При необходимости сюда можно добавить отправку приветственного сообщения.
    """
    logger.info(
        "Moderator telegram linked",
        extra={
            "event": "moderator.telegram_linked",
            "moderator_id": event.data.get("moderator_id"),
            "login": event.data.get("login"),
            "telegram_id": event.data.get("telegram_id"),
            "request_id": extra.get("request_id"),
        },
    )


# --- События мастеров ---
@event_handler('master.created')
async def master_created_handler(event: Event, extra: dict):
    """
    Событие: создан мастер (ещё не верифицирован).

    Сейчас только логируем. Можно расширить до уведомлений.
    """
    logger.info(
        "Master created",
        extra={
            "event": "master.created",
            "master_id": event.data.get("master_id"),
            "name": event.data.get("name"),
            "organization_id": event.data.get("organization_id"),
            "request_id": extra.get("request_id"),
        },
    )

@event_handler('master.updated')
async def master_updated_handler(event: Event, extra: dict):
    """
    Событие: мастер обновлён.
    """
    logger.info(
        "Master updated",
        extra={
            "event": "master.updated",
            "master_id": event.data.get("master_id"),
            "request_id": extra.get("request_id"),
        },
    )

@event_handler('master.deleted')
async def master_deleted_handler(event: Event, extra: dict):
    """
    Событие: мастер удалён.
    """
    logger.info(
        "Master deleted",
        extra={
            "event": "master.deleted",
            "master_id": event.data.get("master_id"),
            "organization_id": event.data.get("organization_id"),
            "deleted_at": event.data.get("deleted_at"),
            "request_id": extra.get("request_id"),
        },
    )

@event_handler('master.telegram_linked')
async def master_telegram_linked_handler(event: Event, extra: dict):
    """
    Событие: мастер привязал Telegram.

    Можно отправить мастеру подтверждение.
    """
    master_id = event.data.get("master_id")
    telegram_id = event.data.get("telegram_id")

    logger.info(
        "Master telegram linked",
        extra={
            "event": "master.telegram_linked",
            "master_id": master_id,
            "telegram_id": telegram_id,
            "request_id": extra.get("request_id"),
        },
    )

    if telegram_id:
        try:
            text = (
                "Вы успешно привязали Telegram-аккаунт.\n"
                "Теперь вы будете получать уведомления о новых бронированиях."
            )
            await master_bot.send_message(chat_id=telegram_id, text=text)
        except Exception as e:
            logger.error(
                "Failed to send master.telegram_linked notification",
                extra={"error": str(e), "telegram_id": telegram_id},
            )


# --- События услуг ---
@event_handler('service.created')
async def service_created_handler(event: Event, extra: dict):
    """
    Событие: создана услуга.
    Пока только логируем.
    """
    logger.info(
        "Service created",
        extra={
            "event": "service.created",
            "service_id": event.data.get("service_id"),
            "master_id": event.data.get("master_id"),
            "request_id": extra.get("request_id"),
        },
    )

@event_handler('service.updated')
async def service_updated_handler(event: Event, extra: dict):
    """
    Событие: услуга обновлена.
    """
    logger.info(
        "Service updated",
        extra={
            "event": "service.updated",
            "service_id": event.data.get("service_id"),
            "request_id": extra.get("request_id"),
        },
    )

@event_handler('service.deleted')
async def service_deleted_handler(event: Event, extra: dict):
    """
    Событие: услуга удалена.
    """
    logger.info(
        "Service deleted",
        extra={
            "event": "service.deleted",
            "service_id": event.data.get("service_id"),
            "request_id": extra.get("request_id"),
        },
    )


# --- События организаций ---

@event_handler('organization.created')
async def organization_created_handler(event: Event, extra: dict):
    """
    Event: create organization

    Waited fields in event.data:
      - title
      - address
      - contact_phone
      - organization_type
      - time_begin
      - time_end
      - organization_id
    """
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    
    # Получаем данные из event.data
    title = event.data.get("title")
    address = event.data.get("address")
    contact_phone = event.data.get("contact_phone")
    organization_type = event.data.get("organization_type")
    time_begin = event.data.get("time_begin")
    time_end = event.data.get("time_end")
    organization_id = event.data.get("organization_id")
    moderator_id = event.data.get('moderator_id')
    moderator_telegram_id = event.data.get("moderator_telegram_id")

    if not all([
        title, 
        address, 
        contact_phone, 
        organization_type, 
        time_begin, 
        time_end, 
        organization_id,
        ]):
        logger.error(
            "organization.created: missing required fields",
            extra={
                "event_data": event.data,
                "required_fields": ["title", "address", "contact_phone", "organization_type", "time_begin", "time_end", "organization_id"]
            },
        )
        return
    
    # Получаем модератора для отправки сообщения
    
    if  not moderator_telegram_id:
        logger.error(
            "organization.created: no moderator found or moderator has no telegram_id",
            extra={"event_data": event.data},
        )
        return
    
    # Формируем ссылку на галерею
    gallery_url = f"https://booking.fix-mst.ru/admin/src/image/?organization__id__exact={organization_id}"
    
    # Формируем сообщение
    message = f"""Новая заявка на верификацию!🟩🟩🟩
Название: {title}
Адрес: {address}
Номер телефона: {contact_phone}
Тип организации: {organization_type}
Начало рабочего дня: {time_begin}
Конец рабочего дня: {time_end}

Ссылка на галерею: {gallery_url}"""
    
    verify_true_button = InlineKeyboardButton(
        "✅ Верифицировать",
        callback_data=f"organization_verify_true_{organization_id}"
    )
    verify_false_button = InlineKeyboardButton(
        "❌ Не верифицировать", 
        callback_data=f"organization_verify_false_{organization_id}"
    )

    inline_markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                verify_true_button, verify_false_button
            ]
        ]
    )
    
    try:
        await moderator_bot.send_message(
            chat_id=moderator_telegram_id,
            text=message,
            reply_markup=inline_markup
        )
        logger.info(
            "Organization creation notification sent to moderator",
            extra={
                "organization_id": organization_id,
                "moderator_id": moderator.id,
                "telegram_id": moderator.telegram_id,
                "request_id": extra.get("request_id"),
            },
        )
    except Exception as e:
        logger.error(
            "Failed to send organization.created notification",
            extra={
                "error": str(e),
                "moderator_telegram_id": moderator_telegram_id,
                "organization_id": organization_id,
            },
        )

    
@event_handler('organization.verified')
async def organization_verified_handler(event: Event, extra: dict):
    """
    Событие: организация прошла верификацию.

    Ожидаемые поля в event.data:
      - organization_telegram_id
    """
    telegram_id = event.data.get("organization_telegram_id")
    if not telegram_id:
        logger.error(
            "organization.verified: missing organization_telegram_id",
            extra={"event_data": event.data},
        )
        return

    from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

    message = (
        "Хорошая новость! ❇️❇️❇️\n"
        "Ваша организация была верифицирована.\n"
        "Теперь вы можете добавлять мастеров и услуг."
    )

    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [
                KeyboardButton("📃 Список мастеров"), 
                KeyboardButton("👥 Список клиентов")
            ],
            [
                KeyboardButton("➕ Добавить мастера")
            ]
        ]
    )
    

    try:
        await organization_bot.send_message(
            chat_id=telegram_id, 
            text=message, 
            reply_markup=kb
        )
    except Exception as e:
        logger.error(
            "Failed to send organization.verified notification",
            extra={"error": str(e), "telegram_id": telegram_id},
        )

@event_handler('organization.rejected')
async def organization_rejected_handler(event: Event, extra: dict):
    """
    Организация не прошла проверку.

    Ожидаемые поля в event.data:
      - organization_telegram_id
    """
    telegram_id = event.data.get("organization_telegram_id")
    if not telegram_id:
        logger.error(
            "organization.rejected: missing organization_telegram_id",
            extra={"event_data": event.data},
        )
        return

    message = (
        "Ваша организация не прошла верификацию!‼️\n"
        "Заполните данные ещё раз - /start"
    )

    try:
        await organization_bot.send_message(chat_id=telegram_id, text=message)
    except Exception as e:
        logger.error(
            "Failed to send organization.rejected notification",
            extra={"error": str(e), "telegram_id": telegram_id},
        )


# --- События клиентов ---
@event_handler('customer.telegram_linked')
async def customer_telegram_linked_handler(event: Event, extra: dict):
    """
    Событие: клиент привязал Telegram (верифицирован).

    Ожидается в event.data:
      - master_telegram_id
      - telegram_id
      - customer_username / username
      - customer_name / name
    """
    master_telegram_id = event.data.get("master_telegram_id")
    customer_telegram_id = event.data.get("telegram_id")

    customer_username = (
        event.data.get("customer_username")
        or event.data.get("username")
    )
    customer_name = (
        event.data.get("customer_name")
        or event.data.get("name")
    )

    if not master_telegram_id:
        logger.error(
            "customer.telegram_linked: missing master_telegram_id",
            extra={"event_data": event.data},
        )
        return

    text = f"✅ Клиент {customer_username or ''} {customer_name or ''} зарегистрировался в системе \n"

    try:
        await master_bot.send_message(chat_id=master_telegram_id, text=text)
    except Exception as e:
        logger.error(
            "Failed to send customer.telegram_linked notification",
            extra={"error": str(e), "master_telegram_id": master_telegram_id},
        )




age = 18
name = "Test"

# str
my_string = 'Книга "1894"'
book_name = str(age)
# int 
money = 10000000
age = 16
students = 15
# bool 
is_active = True or False
# float
pi = 3.14
bi = 0
mass = 71.50
long = 500.9
height = 182.90
number = "89238928392"

# list
users = [
    1,2,3
]
# /start
users.append(message)


dict
set
tuple