# pylint: disable=too-many-arguments, too-many-positional-arguments
"""
Уведомления
"""

import time


import logging

from celery import shared_task
from celery.signals import task_prerun, task_postrun, task_retry, task_failure
from telebot import types

from bot.config import master_bot, moderator_bot, organization_bot
from src.models import Master, Organization, Moderator, Customer
from src.utils.logger import RequestLogger


task_start_times = {}


# pylint: disable=unused-argument
@task_prerun.connect
def task_prerun_hanlder(sender=None, task_id=None, task=None, **kwargs):
    """Выполнение действия перед запуском задачи"""
    task_start_times[task_id] = time.time()


@task_postrun.connect
def task_postrun_hanlder(sender=None, task_id=None, task=None, **kwargs):
    """Выполнение действия после запуска задачи"""
    logger = logging.getLogger("src.celery")
    start_time = task_start_times.get(task_id, None)
    if start_time:
        duration = time.time() - start_time
        if duration > 30.0:  # больше 5 секунд
            logger.warning(
                "Длительная Celery-задача",
                extra={
                    "task_name": sender.name,
                    "task_id": task_id,
                    "duration": round(duration, 3),
                    "event": "celery.slow.task",
                },
            )


@task_retry.connect
def task_retry_handler(sender=None, reason=None, **kwargs):
    """Повторная попытка выполнения задачи"""
    logger = logging.getLogger("src.celery")
    logger.warning(
        "Повторная попытка выполнения задачи",
        extra={
            "task_id": kwargs.get("task_id"),
            "task_name": sender.name,
            "reason": str(reason),
            "retries": kwargs.get("request", {}).get("retries", 0),
            "event": "celery.task.retry",
        },
    )


@task_failure.connect
def on_task_failure(
    sender=None, task_id=None, exception=None, traceback=None, **kwargs
):
    """Логируем ошибки задач"""
    logger = logging.getLogger("src.celery")
    logger.error(
        "Ошибка в Celery задаче",
        extra={
            "task_name": sender.name,
            "task_id": task_id,
            "exception_type": type(exception).__name__,
            "exception_message": str(exception),
            "traceback": str(traceback)[:500],
            "event": "celery.task.failed",
        },
    )


def callback_verify_organization(organization_id: int, is_verify: bool) -> str:
    """Генерация callback data"""
    return f"organization_verify_{'true' if is_verify else 'false'}_{organization_id}"


def get_moderator_for_send_message() -> Moderator:
    """Получение модератора"""
    return Moderator.objects.first()


@shared_task(bind=True)
def send_message_telegram_on_master(
    self,
    master_id: int,
    client_phone_number: str,
    booking_date: str,
    booking_time: str,
    request_id: str,
):
    """
    Отправка сообщения Мастеру о бронировании
    """
    logger = RequestLogger(request_id)

    try:
        master = Master.objects.get(id=master_id)
        logger.info(
            "Началась отправка телеграм сообщения",
            extra={
                "master_id": master.pk,
                "master_telegram_id": master.telegram_id,
                "event": "notify.send.telegram",
            },
        )

        text = (
            f"У вас новая бронь\n"
            f"Клиент: {client_phone_number}\n"
            f"Дата: {booking_date}\n"
            f"Время: {booking_time}"
        )

        master_bot.send_message(chat_id=master.telegram_id, text=text)
        logger.debug(
            "Сообщение о бронировании успешно отправлено",
            extra={
                "master_id": master.pk,
                "master_telegram_id": master.telegram_id,
                "event": "notify.send.telegram",
            },
        )
    except Exception as error:
        logger.error(
            "Ошибка отправки телеграм сообщения, пробуем снова",
            extra={
                "error_message": f"{error}",
                "master_id": master.telegram_id,
                "event": "notify.send.telegram",
            },
        )
        raise self.retry(error, countdown=10, max_retries=3)


@shared_task
def send_message_about_verify_master(
    master_id: int,
):
    """
    Отправка уведомления мастеру о его верификации
    """
    master = Master.objects.get(id=master_id)
    text = f"✅ Мастер {master.name} {master.surname} зарегистрировался в системе \n"
    organization_bot.send_message(chat_id=master.organization.telegram_id, text=text)


@shared_task
def send_message_about_verify_customer(master_id: int, customer_id: int):
    """
    Отправка уведомления клиенту о его верификации

    """
    customer = Customer.objects.get(telegram_id=customer_id)
    text = (
        f"✅ Клиент {customer.username} {customer.name} зарегистрировался в системе \n"
    )
    master_bot.send_message(chat_id=master_id, text=text)


@shared_task
def send_message_on_moderator_about_organization(organization_id: int):
    """Отправка сообщения модератору о регистрации новой организации"""
    organization = Organization.objects.get(pk=organization_id)
    moderator = get_moderator_for_send_message()
    gallery_url = "https://booking.fix-mst.ru/admin/src/image/?organization__id__exact="
    message = f"""Новая заявка на верификацию!🟩🟩🟩
        Название: {organization.title}
        Адрес: {organization.address}
        Номер телефона: {organization.contact_phone}
        Тип огранизации: {organization.organization_type.title}
        Начало рабочего дня: {organization.time_begin}
        Конец рабочего дня: {organization.time_end}\n
        Ссылка на галерею: {gallery_url}{organization.pk}"""

    moderator_inline_markup = types.InlineKeyboardMarkup()
    verify_true_button = types.InlineKeyboardButton(
        "✅ Верифицировать",
        callback_data=callback_verify_organization(organization_id, True),
    )
    verify_false_button = types.InlineKeyboardButton(
        "❌ Не верифицировать",
        callback_data=callback_verify_organization(organization_id, False),
    )
    moderator_inline_markup.add(verify_true_button)
    moderator_inline_markup.add(verify_false_button)
    moderator_bot.send_message(
        chat_id=moderator.telegram_id,
        text=message,
        reply_markup=moderator_inline_markup,
    )


@shared_task()
def send_is_verified_organization(
    organization_id: int,
    is_verify: bool,
):
    """Отправка сообщения о регистрации новой организации"""
    organization = Organization.objects.get(pk=organization_id)
    if is_verify:
        message = """Хорошая новость! ❇️❇️❇️
Ваша организация была верифицировна.
Теперь вы можете добавлять мастеров и услуг.
        """
        organization_menu_markup = types.ReplyKeyboardMarkup()

        master_list = types.KeyboardButton("📃 Список мастеров")
        client_list = types.KeyboardButton("👥 Список клиентов")
        add_master = types.KeyboardButton("➕ Добавить мастера")

        organization_menu_markup.add(master_list)
        organization_menu_markup.add(client_list)
        organization_menu_markup.add(add_master)

        organization_bot.send_message(
            chat_id=organization.telegram_id,
            text=message,
            reply_markup=organization_menu_markup,
        )
    else:
        message = (
            "Ваша организация не прошла верификацию!‼️\n"
            "Заполните данные еще раз - /start"
        )
        organization_bot.send_message(
            chat_id=organization.telegram_id,
            text=message,
        )
