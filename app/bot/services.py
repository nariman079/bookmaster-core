from datetime import datetime
import socket

from django.db.models import F
from django.db.transaction import atomic
from config.settings import cache
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response


from src.utils.logger import RequestLogger
from src.serializers.organization_serializers import OrganizationDetailSerializer
from src.models import Customer, Organization, Master, Moderator, Service, Image
from src.tasks import (
    send_message_on_moderator_about_organization,
    send_message_about_verify_master,
    send_is_verified_organization,
    send_message_about_verify_customer,
    send_message_in_broker,
)


def check_organization_exist(contact_phone) -> Response:
    """Проверка существования организации в БД"""
    organization = Organization.objects.filter(contact_phone=contact_phone).first()
    if organization:
        raise ValidationError(
            {
                "message": "Такая организация уже есть в системе",
                "success": False,
                "data": [],
            },
            code=400,
        )
    return Response(
        {"message": "Такой организации нет в системе", "success": True, "data": []},
        status=200,
    )


def next_session(start_date, start_time):
    start_date_time_str = f"{start_date} {start_time}"
    start_date_time = datetime.strptime(start_date_time_str, "%Y-%m-%d %H:%M:%S")

    current_date_time = datetime.now()

    result_text = "Следующая процедура через\n"

    time_difference = start_date_time - current_date_time

    total_seconds = int(time_difference.total_seconds())
    days, seconds = divmod(total_seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes = seconds // 60

    months = days // 30
    days = days % 30

    if months > 0:
        result_text += f" {months} месяцев,"
    if days > 0:
        result_text += f" {days} дней,"
    if hours > 0:
        result_text += f" {hours} часов,"

    output = f"{result_text} {minutes} минут."

    return output


class BaseService:
    def __init__(self, logger: None | RequestLogger = None):
        self.logger = logger or RequestLogger(request_id="untracked")


class GetProfile(BaseService):
    def __init__(self, data: dict, logger: None | RequestLogger = None):
        super().__init__(logger)
        try:
            self.phone_number = data["phone_number"]
            self.username = data["username"]
            self.telegram_id = data["telegram_id"]
            self.user_keyword = data["user_keyword"]
        except Exception as error:
            raise ValidationError(
                {
                    "message": "Неизвествная ошибка\nОбратитесь к администратору @nariman079i",
                    "success": False,
                    "data": "\n".join(error.args),
                },
                code=422,
            )

    def _get_customer_from_db(self) -> None:
        """
        Получение клиента из базы данных
        """
        self.customer = Customer.objects.filter(phone=self.phone_number).first()
        if not self.customer:
            msg = "Такого клиента нет в системе"
            self.logger.warning(
                msg,
                extra={
                    "telegram_id": self.telegram_id,
                    "event": "telegram.get_profile",
                },
            )
            raise ValidationError({"message": msg})

    def _check_user_keyword(self):
        """Проверка ключегово слова"""
        if self.customer.user_keyword != self.user_keyword:
            msg = "Вы неправильно указали слово пароля"

            self.logger.warning(
                msg,
                extra={
                    "event": "telegram.get_profile",
                    "telegram_id": self.customer.telegram_id,
                },
            )

            raise ValidationError(
                {
                    "message": msg,
                    "success": False,
                    "data": [],
                },
                code=400,
            )

    def _fill_telegram_user_data(self) -> None:
        """
        Заполнение данных
        """
        self.customer.username = self.username
        self.customer.telegram_id = self.telegram_id
        self.customer.save()

    def execute(self):
        self._get_customer_from_db()
        self._check_user_keyword()
        self._fill_telegram_user_data()

        msg = "Вы успешно авторизовались\nВы будете получать уведомления о брони"
        self.logger.success(
            msg,
            extra={
                "event": "telegram.get_profile",
                "telegram_id": self.customer.telegram_id,
            },
        )
        return Response(
            {
                "message": msg,
                "success": True,
            }
        )


class BotOrganizationCreate(BaseService):
    """Создание организации"""

    def __init__(self, organization_data: dict, logger: None | RequestLogger = None):
        super().__init__(logger)
        try:
            self.organization = organization_data.copy()
            self.gallery = self.organization.pop("gallery", [])
        except Exception as e:
            self.logger.error(
                "Ошибка при извлечении данных организации из входного payload",
                extra={
                    "event": "telegram.organization_create.init_fail",
                    "error": str(e),
                    "payload_keys": list(organization_data.keys())
                    if hasattr(organization_data, "keys")
                    else "not dict",
                },
            )
            raise ValidationError(
                {
                    "message": "Неизвестная ошибка\nОбратитесь к администратору @nariman079i",
                    "success": False,
                    "data": {},
                },
                code=422,
            )

    def _create_organization(self):
        self.logger.info(
            "Начало создания новой организации",
            extra={
                "event": "telegram.organization_create.start",
                "contact_phone": self.organization.get("contact_phone"),
                "title": self.organization.get("title"),
                "has_gallery": bool(self.gallery),
            },
        )
        try:
            self.organization_obj: Organization = Organization.objects.create(
                **self.organization
            )
            self.logger.info(
                "Организация успешно создана в БД",
                extra={
                    "event": "telegram.organization_create.db_success",
                    "organization_id": self.organization_obj.pk,
                    "contact_phone": self.organization_obj.contact_phone,
                },
            )

        except Exception as error:
            self.logger.error(
                "Ошибка при сохранении организации в базу данных",
                extra={
                    "event": "telegram.organization_create.db_fail",
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                    "organization_data_keys": list(self.organization.keys()),
                    "contact_phone": self.organization.get("contact_phone"),
                },
            )
            raise ValidationError(
                {
                    "message": "Не удалось создать организацию. Обратитесь к администратору @nariman079i",
                    "success": False,
                    "data": {"error": str(error)},
                },
                code=422,
            )

    def _create_gallery(self):
        if not self.gallery:
            self.logger.debug(
                "Галерея отсутствует — пропускаем создание изображений",
                extra={"event": "telegram.organization_create.gallery_skip"},
            )
            return

        self.logger.info(
            "Начало сохранения изображений галереи",
            extra={
                "event": "telegram.organization_create.gallery_start",
                "image_count": len(self.gallery),
                "organization_id": self.organization_obj.pk,
            },
        )

        created = 0
        for idx, image_url in enumerate(self.gallery):
            try:
                Image.objects.create(
                    organization=self.organization_obj, image_url=image_url
                )
                created += 1
            except Exception as e:
                self.logger.warning(
                    "Не удалось сохранить изображение из галереи",
                    extra={
                        "event": "telegram.organization_create.image_fail",
                        "organization_id": self.organization_obj.pk,
                        "image_index": idx,
                        "image_url": image_url,
                        "error": str(e),
                    },
                )
                # Не прерываем — остальные изображения могут быть валидны

        self.logger.info(
            "Сохранение галереи завершено",
            extra={
                "event": "telegram.organization_create.gallery_done",
                "organization_id": self.organization_obj.pk,
                "total_requested": len(self.gallery),
                "successfully_created": created,
            },
        )
    
    def get_moderator_for_send_message(self) -> Moderator:
        """Получение модератора"""
        return Moderator.objects.first()

    def _send_notification(self):
        try:
            self.logger.info(
                "Отправка уведомления модератору о новой организации",
                extra={
                    "event": "telegram.organization_create.notify_start",
                    "organization_id": self.organization_obj.pk,
                },
            )

            # send_message_on_moderator_about_organization.delay(self.organization_obj.pk)
            moderator = self.get_moderator_for_send_message()
            message_data = {
                "event": "order.create",
                "notification_type": "telegram",
                "data": {
                  
                    "title": self.organization_obj.title,
                    "address": self.organization_obj.address,
                    "contact_phone": self.organization_obj.contact_phone,
                    "organization_type": self.organization_obj.organization_type.title,
                    "time_begin": self.organization_obj.time_begin.isoformat(),
                    "time_end": self.organization_obj.time_end.isoformat(),
                    "organization_id": self.organization_obj.pk,
                    "moderator_id": moderator.pk,
                    "moderator_telegram_id": moderator.telegram_id,
                },
                "metadata": {
                    "request_id": self.logger.request_id,
                    "timestamp": timezone.now().isoformat(),
                    "source": "bookingmaster-bot",
                    "instance_id": socket.gethostname(),
                },
                "_from": "bot",
            }
            send_message_in_broker.delay(message_data)
            

            self.logger.info(
                "Уведомление модератору поставлено в очередь Celery",
                extra={
                    "event": "telegram.organization_create.notify_queued",
                    "organization_id": self.organization_obj.pk,
                },
            )
        except Exception as e:
            self.logger.error(
                "Ошибка при постановке задачи уведомления модератору в очередь",
                extra={
                    "event": "telegram.organization_create.notify_fail",
                    "organization_id": self.organization_obj.pk,
                    "error": str(e),
                },
            )

    @atomic()
    def execute(self):
        self._create_organization()
        self._create_gallery()
        # self._send_notification()  # раскомментировать при включении

        self.logger.info(
            "Процесс регистрации организации успешно завершён",
            extra={
                "event": "telegram.organization_create.success",
                "organization_id": self.organization_obj.pk,
                "contact_phone": self.organization_obj.contact_phone,
                "gallery_images_count": len(self.gallery),
            },
        )

        msg = "Вы успешно зарегистрировали организацию. Ожидайте подтверждения от модератора."

        return Response(
            {
                "message": msg,
                "success": True,
                "data": {**self.organization, "id": self.organization_obj.pk},
            },
            status=201,
        )


class BotMasterGetProfile(BaseService):
    """Привязка Telegram-аккаунта к мастеру по имени и коду (авторизация мастера в боте)"""

    def __init__(self, master_data: dict, logger: RequestLogger | None = None):
        super().__init__(logger=logger)
        try:
            self.telegram_id = master_data["telegram_id"]
            self.code = str(master_data["code"]).strip()  # ← защита от int/float
            self.name = master_data["name"].strip()

            self.logger.info(
                "Начало авторизации мастера через Telegram",
                extra={
                    "event": "telegram.master.auth.start",
                    "telegram_id": self.telegram_id,
                    "master_name": self.name,
                    "code_length": len(self.code),
                },
            )

        except KeyError as e:
            missing_key = str(e).strip("'")
            self.logger.error(
                "Отсутствует обязательное поле во входных данных",
                extra={
                    "event": "telegram.master.auth.init_fail",
                    "missing_key": missing_key,
                    "provided_keys": list(master_data.keys()),
                },
            )
            raise ValidationError(
                {
                    "message": f"Не указано поле: {missing_key}",
                    "success": False,
                },
                code=400,
            )
        except Exception as e:
            self.logger.exception(
                "Неожиданная ошибка при инициализации данных мастера",
                extra={
                    "event": "telegram.master.auth.init_error",
                    "error": str(e),
                },
            )
            raise ValidationError(
                {
                    "message": "Некорректные входные данные",
                    "success": False,
                },
                code=422,
            )

    def _get_master(self):
        self.logger.debug(
            "Поиск мастера по имени",
            extra={"event": "telegram.master.auth.lookup", "master_name": self.name},
        )

        master = Master.objects.filter(name=self.name).first()

        if not master:
            self.logger.warning(
                "Мастер с указанным именем не найден",
                extra={
                    "event": "telegram.master.auth.not_found",
                    "master_name": self.name,
                },
            )
            raise ValidationError(
                {
                    "message": "Мастер с таким именем не зарегистрирован",
                    "success": False,
                },
                code=404,
            )

        if master.code != self.code:
            self.logger.warning(
                "Неверный код подтверждения для мастера",
                extra={
                    "event": "telegram.master.auth.code_mismatch",
                    "master_id": master.pk,
                    "master_name": master.name,
                    "provided_code_length": len(self.code),
                    "expected_code_length": len(str(master.code)),
                },
            )
            raise ValidationError(
                {"message": "Неверный код подтверждения", "success": False},
                code=400,
            )

        old_telegram_id = master.telegram_id
        master.telegram_id = self.telegram_id
        master.save(update_fields=["telegram_id"])

        self.master = master

        self.logger.info(
            "Telegram-аккаунт успешно привязан к мастеру",
            extra={
                "event": "telegram.master.auth.success",
                "master_id": master.pk,
                "master_name": master.name,
                "telegram_id": self.telegram_id,
                "was_relinked": bool(
                    old_telegram_id and old_telegram_id != self.telegram_id
                ),
                "had_telegram_before": bool(old_telegram_id),
            },
        )

    def _publish_event(self):
        """Отправка уведомления в брокер о том что аккаунт привязян"""
        # TODO send_message_in_broker()
        pass

    def execute(self):
        self._get_master()
        self._publish_event()

        master_data = {
            "id": self.master.pk,
            "name": self.master.name,
            "telegram_id": self.master.telegram_id,
        }

        self.logger.info(
            "Авторизация мастера завершена успешно",
            extra={
                "event": "telegram.master.auth.complete",
                "master_id": self.master.pk,
                "telegram_id": self.telegram_id,
            },
        )

        return Response(
            {
                "message": "Вы успешно авторизованы как мастер.\nТеперь вы будете получать уведомления о новых бронированиях.",
                "success": True,
                "data": master_data,
            },
            status=200,
        )


class BotModeratorGetProfile(BaseService):
    """Привязка Telegram-аккаунта к модератору по логину и коду (авторизация в боте)"""

    def __init__(self, moderator_data: dict, logger: RequestLogger | None = None):
        super().__init__(logger=logger)
        try:
            self.telegram_id = int(moderator_data["telegram_id"])
            self.code = str(moderator_data["code"]).strip()
            self.login = str(moderator_data["login"]).strip()

            self.logger.info(
                "Начало авторизации модератора через Telegram",
                extra={
                    "event": "telegram.moderator.auth.start",
                    "telegram_id": self.telegram_id,
                    "login": self.login,
                    "code_length": len(self.code),
                },
            )

        except (KeyError, ValueError, TypeError) as e:
            field = (
                getattr(e, "args", [None])[0] if isinstance(e, KeyError) else "unknown"
            )
            self.logger.warning(
                "Некорректные входные данные при авторизации модератора",
                extra={
                    "event": "telegram.moderator.auth.init_invalid",
                    "error_type": type(e).__name__,
                    "field": field,
                    "data_keys": list(moderator_data.keys()),
                },
            )
            raise ValidationError(
                {"message": "Неверный формат данных", "success": False},
                code=400,
            )
        except Exception as e:
            self.logger.exception(
                "Необработанная ошибка при инициализации данных модератора",
                extra={"event": "telegram.moderator.auth.init_fail"},
            )
            raise ValidationError(
                {"message": "Внутренняя ошибка сервера", "success": False},
                code=500,
            )

    def _get_moderator(self):
        self.logger.debug(
            "Поиск модератора по логину",
            extra={"event": "telegram.moderator.auth.lookup", "login": self.login},
        )

        moderator = Moderator.objects.filter(login=self.login).first()

        if not moderator:
            self.logger.warning(
                "Модератор с указанным логином не найден",
                extra={
                    "event": "telegram.moderator.auth.not_found",
                    "login": self.login,
                },
            )
            raise ValidationError(
                {
                    "message": "Модератор с таким логином не зарегистрирован",
                    "success": False,
                },
                code=404,
            )

        if moderator.telegram_id == self.telegram_id:
            self.logger.info(
                "Модератор уже авторизован с этим Telegram-аккаунтом",
                extra={
                    "event": "telegram.moderator.auth.already_linked",
                    "moderator_id": moderator.pk,
                    "login": self.login,
                    "telegram_id": self.telegram_id,
                },
            )
            raise ValidationError(
                {"message": "Вы уже зарегистрированы в системе", "success": True},
                code=200,
            )

        if str(moderator.code).strip() != self.code:
            self.logger.warning(
                "Неверный код подтверждения для модератора",
                extra={
                    "event": "telegram.moderator.auth.code_mismatch",
                    "moderator_id": moderator.pk,
                    "login": self.login,
                    "provided_code_sample": self.code[:3] + "..."
                    if len(self.code) > 3
                    else self.code,
                    "expected_code_length": len(str(moderator.code)),
                },
            )
            raise ValidationError(
                {
                    "message": "Вы неправильно ввели код. Попробуйте ещё раз",
                    "success": False,
                },
                code=400,
            )

        old_telegram_id = moderator.telegram_id
        moderator.telegram_id = self.telegram_id
        moderator.save(update_fields=["telegram_id"])

        self.moderator = moderator

        self.logger.info(
            "Telegram-аккаунт успешно привязан к модератору",
            extra={
                "event": "telegram.moderator.auth.success",
                "moderator_id": moderator.pk,
                "login": self.login,
                "telegram_id": self.telegram_id,
                "was_relinked": bool(
                    old_telegram_id and old_telegram_id != self.telegram_id
                ),
            },
        )

    def _publish_event(self):
        """Отправка события в Kafka/брокер"""
        event_data = {
            "event": "moderator.telegram_linked",
            "notification_type": "internal",
            "data": {
                "moderator_id": self.moderator.pk,
                "login": self.moderator.login,
                "telegram_id": self.telegram_id,
                "timestamp": datetime.utcnow().isoformat(),
            },
            "metadata": {
                "request_id": self.logger.request_id
                if hasattr(self.logger, "request_id")
                else "unknown",
                "source": "bookingmaster-api",
                "service": "bot",
            },
        }

        try:
            self.logger.debug(
                "Постановка события авторизации модератора в очередь брокера",
                extra={
                    "event": "telegram.moderator.auth.event_queued",
                    "moderator_id": self.moderator.pk,
                },
            )

            send_message_in_broker.delay(event_data, "internal")

            self.logger.info(
                "Событие авторизации модератора отправлено в брокер",
                extra={
                    "event": "telegram.moderator.auth.event_sent",
                    "moderator_id": self.moderator.pk,
                    "broker_task": "send_message_in_broker",
                },
            )
        except Exception as e:
            self.logger.error(
                "Не удалось отправить событие в брокер",
                extra={
                    "event": "telegram.moderator.auth.event_fail",
                    "moderator_id": self.moderator.pk,
                    "error": str(e),
                },
            )

    def execute(self):
        self._get_moderator()
        self._publish_event()

        return Response(
            {
                "message": (
                    "Вы успешно авторизованы как модератор.\n"
                    "Теперь вы будете получать заявки на верификацию организаций."
                ),
                "success": True,
                "data": {
                    "moderator_id": self.moderator.pk,
                    "login": self.moderator.login,
                },
            },
            status=200,
        )


class BotVerifyOrganization(BaseService):
    """Верификация (подтверждение/отклонение) организации модератором"""

    def __init__(
        self,
        verify_organization_data: dict,
        logger: RequestLogger | None = None,
        moderator_id: int | None = None,
    ):
        super().__init__(logger=logger)
        self.moderator_id = moderator_id

        try:
            self.organization_id = int(verify_organization_data["organization_id"])
            self.is_verify = verify_organization_data["is_verify"]

            if not isinstance(self.is_verify, bool):
                raise ValueError(
                    f"'is_verify' must be boolean, got {type(self.is_verify).__name__}: {self.is_verify}"
                )

            self.logger.info(
                "Начало обработки решения по верификации организации",
                extra={
                    "event": "organization.verify.request",
                    "organization_id": self.organization_id,
                    "is_verify": self.is_verify,
                    "moderator_id": self.moderator_id,
                },
            )

        except (KeyError, ValueError, TypeError) as e:
            field = (
                getattr(e, "args", [None])[0]
                if isinstance(e, KeyError)
                else "is_verify"
            )
            self.logger.warning(
                "Некорректные данные для верификации организации",
                extra={
                    "event": "organization.verify.invalid_input",
                    "error": str(e),
                    "field": field,
                    "data": verify_organization_data,
                },
            )
            raise ValidationError(
                {
                    "message": "Неверный формат данных: требуется organization_id (int) и is_verify (bool)",
                    "success": False,
                },
                code=400,
            )
        except Exception as e:
            self.logger.exception(
                "Необработанная ошибка при инициализации верификации",
                extra={"event": "organization.verify.init_fail"},
            )
            raise ValidationError(
                {"message": "Внутренняя ошибка сервера", "success": False},
                code=500,
            )

    def get_organization(self):
        self.logger.debug(
            "Поиск организации по ID",
            extra={
                "event": "organization.verify.lookup",
                "organization_id": self.organization_id,
            },
        )

        self.organization = Organization.objects.filter(pk=self.organization_id).first()

        if not self.organization:
            self.logger.warning(
                "Организация не найдена при попытке верификации",
                extra={
                    "event": "organization.verify.not_found",
                    "organization_id": self.organization_id,
                },
            )
            raise ValidationError(
                {"message": "Организация не найдена", "success": False},
                code=404,
            )

        self.logger.info(
            "Организация найдена",
            extra={
                "event": "organization.verify.found",
                "organization_id": self.organization_id,
                "current_is_verified": self.organization.is_verified,
                "contact_phone": self.organization.contact_phone,
                "title": self.organization.title,
            },
        )

    def check_and_update_verify_status(self):
        if self.organization.is_verified == self.is_verify:
            self.logger.info(
                "Статус верификации не изменился — пропускаем обновление",
                extra={
                    "event": "organization.verify.no_change",
                    "organization_id": self.organization_id,
                    "is_verify": self.is_verify,
                },
            )
            return False  # → не было изменений

        old_status = self.organization.is_verified
        self.organization.is_verified = self.is_verify
        self.organization.verified_at = timezone.now() if self.is_verify else None
        # self.organization.verified_by_id = self.moderator_id  # ← если есть FK verified_by
        self.organization.save(
            update_fields=["is_verified", "verified_at", "verified_by_id"]
        )

        self.logger.info(
            "Статус верификации организации обновлён",
            extra={
                "event": "organization.verify.updated",
                "organization_id": self.organization_id,
                "old_status": old_status,
                "new_status": self.is_verify,
                "moderator_id": self.moderator_id,
            },
        )
        return True

    def send_notification_to_org(self):
        """Отправка уведомления самой организации (в бот)"""
        try:
            self.logger.debug(
                "Постановка задачи отправки уведомления организации",
                extra={
                    "event": "organization.verify.notify_org_queued",
                    "organization_id": self.organization_id,
                },
            )

            send_is_verified_organization.delay(self.organization.id, self.is_verify)

            self.logger.info(
                "Уведомление организации поставлено в очередь",
                extra={
                    "event": "organization.verify.notify_org_sent",
                    "organization_id": self.organization_id,
                },
            )
        except Exception as e:
            self.logger.error(
                "Ошибка при постановке уведомления организации в очередь",
                extra={
                    "event": "organization.verify.notify_org_fail",
                    "organization_id": self.organization_id,
                    "error": str(e),
                },
            )

    def _publish_event(self):
        """Отправка события в Kafka: организация верифицирована/отклонена"""
        event_name = (
            "organization.verified" if self.is_verify else "organization.rejected"
        )

        event_data = {
            "event": event_name,
            "data": {
                "organization_id": self.organization.id,
                "title": self.organization.title,
                "contact_phone": self.organization.contact_phone,
                "is_verified": self.is_verify,
                "verified_at": self.organization.verified_at.isoformat()
                if self.organization.verified_at
                else None,
                "moderator_id": self.moderator_id,
            },
            "metadata": {
                "request_id": getattr(self.logger, "request_id", "unknown"),
                "timestamp": timezone.now().isoformat(),
                "source": "bookingmaster-api",
                "service": "moderation",
            },
        }

        try:
            self.logger.debug(
                "Постановка события верификации в очередь брокера",
                extra={
                    "event": "organization.verify.event_queued",
                    "organization_id": self.organization.id,
                },
            )
            send_message_in_broker.delay(event_data)
            self.logger.info(
                f"Событие '{event_name}' отправлено в брокер",
                extra={
                    "event": "organization.verify.event_sent",
                    "organization_id": self.organization.id,
                    "event_name": event_name,
                },
            )
        except Exception as e:
            self.logger.error(
                "Не удалось отправить событие верификации в брокер",
                extra={
                    "event": "organization.verify.event_fail",
                    "organization_id": self.organization.id,
                    "error": str(e),
                },
            )

    @atomic
    def execute(self):
        self.get_organization()
        status_changed = self.check_and_update_verify_status()

        # Отправляем уведомления и события **только если статус изменился**
        if status_changed:
            self.send_notification_to_org()
            self._publish_event()

        action = "подтверждена" if self.is_verify else "отклонена"
        message = f"Организация «{self.organization.title}» успешно {action}."

        return Response(
            {
                "message": message,
                "success": True,
                "data": {
                    "organization_id": self.organization.id,
                    "is_verified": self.organization.is_verified,
                    "title": self.organization.title,
                },
            },
            status=200,
        )


class BotGetOrganizationByTelegramId(BaseService):
    """Проверка: привязана ли организация к указанному Telegram ID (для регистрации нового аккаунта)"""

    def __init__(self, organization_data: dict, logger: RequestLogger | None = None):
        super().__init__(logger=logger)
        try:
            self.telegram_id = int(organization_data["telegram_id"])
            self.logger.debug(
                "Начало проверки существования организации по Telegram ID",
                extra={
                    "event": "telegram.organization.check.start",
                    "telegram_id": self.telegram_id,
                },
            )
        except (KeyError, ValueError, TypeError) as e:
            self.logger.warning(
                "Некорректный telegram_id в запросе проверки организации",
                extra={
                    "event": "telegram.organization.check.invalid_input",
                    "error": str(e),
                    "input_data": organization_data,
                },
            )
            raise ValidationError(
                {"message": "Неверный формат telegram_id", "success": False},
                code=400,
            )

    def get_organization(self):
        self.organization = Organization.objects.filter(
            telegram_id=self.telegram_id
        ).first()

        if self.organization:
            self.logger.info(
                "Организация с указанным Telegram ID уже существует",
                extra={
                    "event": "telegram.organization.check.exists",
                    "telegram_id": self.telegram_id,
                    "organization_id": self.organization.pk,
                    "contact_phone": self.organization.contact_phone,
                },
            )
        else:
            self.logger.debug(
                "Организация с указанным Telegram ID не найдена",
                extra={
                    "event": "telegram.organization.check.not_found",
                    "telegram_id": self.telegram_id,
                },
            )

    def check_organization(self):
        if self.organization:
            raise ValidationError(
                {
                    "message": "Этот Telegram-аккаунт уже привязан к организации в системе",
                    "success": False,
                },
                code=409,
            )

    def execute(self):
        self.get_organization()
        self.check_organization()

        self.logger.info(
            "Проверка пройдена: Telegram ID свободен для регистрации новой организации",
            extra={
                "event": "telegram.organization.check.free",
                "telegram_id": self.telegram_id,
            },
        )

        return Response(
            {
                "message": "Аккаунт не найден — вы можете зарегистрировать новую организацию",
                "success": True,
                "data": {},
            },
            status=200,
        )


class MasterDeleteSrv(BaseService):
    """Удаление мастера (мягкое или полное — в зависимости от бизнес-логики)"""

    def __init__(self, master_id: int, logger: RequestLogger | None = None):
        super().__init__(logger=logger)
        try:
            self.master_id = int(master_id)
            if self.master_id <= 0:
                raise ValueError("master_id must be positive")
            self.logger.info(
                "Начало удаления мастера",
                extra={
                    "event": "master.delete.request",
                    "master_id": self.master_id,
                },
            )
        except (ValueError, TypeError) as e:
            self.logger.warning(
                "Некорректный master_id при удалении",
                extra={
                    "event": "master.delete.invalid_id",
                    "master_id_raw": master_id,
                    "error": str(e),
                },
            )
            raise ValidationError(
                {"message": "Неверный идентификатор мастера", "success": False},
                code=400,
            )

    def get_master(self):
        self.logger.debug(
            "Поиск мастера по ID",
            extra={"event": "master.delete.lookup", "master_id": self.master_id},
        )

        self.master = (
            Master.objects.select_related("organization")
            .filter(pk=self.master_id)
            .first()
        )

        if not self.master:
            self.logger.warning(
                "Попытка удаления несуществующего мастера",
                extra={"event": "master.delete.not_found", "master_id": self.master_id},
            )
            raise ValidationError(
                {"message": "Мастер не найден", "success": False},
                code=404,
            )

        self.logger.info(
            "Мастер найден для удаления",
            extra={
                "event": "master.delete.found",
                "master_id": self.master.pk,
                "master_name": self.master.name,
                "organization_id": self.master.organization_id,
                "organization_telegram_id": getattr(
                    self.master.organization, "telegram_id", None
                ),
            },
        )

    def _invalidate_cache(self):
        try:
            org_telegram_id = getattr(self.master.organization, "telegram_id", None)
            if org_telegram_id:
                cache_key = str(org_telegram_id)
                cache.delete(cache_key)
                self.logger.debug(
                    "Кэш организации инвалидирован",
                    extra={
                        "event": "master.delete.cache_invalidated",
                        "master_id": self.master_id,
                        "organization_telegram_id": org_telegram_id,
                        "cache_key": cache_key,
                    },
                )
            else:
                self.logger.debug(
                    "Организация мастера не имеет telegram_id — пропуск инвалидации кэша",
                    extra={
                        "event": "master.delete.cache_skip",
                        "master_id": self.master_id,
                    },
                )
        except Exception as e:
            self.logger.error(
                "Ошибка при инвалидации кэша после удаления мастера",
                extra={
                    "event": "master.delete.cache_fail",
                    "master_id": self.master_id,
                    "error": str(e),
                },
            )

    def _publish_event(self):
        """Отправка события 'master.deleted' в Kafka"""
        event_data = {
            "event": "master.deleted",
            "data": {
                "master_id": self.master.pk,
                "name": self.master.name,
                "organization_id": self.master.organization_id,
                "organization_telegram_id": getattr(
                    self.master.organization, "telegram_id", None
                ),
                "deleted_at": datetime.utcnow().isoformat(),
            },
            "metadata": {
                "request_id": getattr(self.logger, "request_id", "unknown"),
                "source": "bookingmaster-api",
                "service": "masters",
            },
        }

        try:
            self.logger.debug(
                "Постановка события удаления мастера в очередь брокера",
                extra={
                    "event": "master.delete.event_queued",
                    "master_id": self.master.pk,
                },
            )
            send_message_in_broker.delay(event_data)
            self.logger.info(
                "Событие удаления мастера отправлено в брокер",
                extra={
                    "event": "master.delete.event_sent",
                    "master_id": self.master.pk,
                },
            )
        except Exception as e:
            self.logger.error(
                "Не удалось отправить событие удаления в брокер",
                extra={
                    "event": "master.delete.event_fail",
                    "master_id": self.master.pk,
                    "error": str(e),
                },
            )

    @atomic
    def delete_master(self):
        deleted_count, _ = self.master.delete()
        if deleted_count == 0:
            self.logger.warning(
                "Мастер не был удалён (возможно, уже удалён ранее)",
                extra={"event": "master.delete.noop", "master_id": self.master_id},
            )
        else:
            self.logger.info(
                "Мастер успешно удалён из БД",
                extra={"event": "master.delete.success", "master_id": self.master_id},
            )

    def execute(self):
        self.get_master()
        self.delete_master()
        self._invalidate_cache()
        self._publish_event()

        return Response(status=204)


class MasterCreateSrv(BaseService):
    """Создание нового мастера (без привязки к Telegram — код выдаётся для последующей верификации)"""

    def __init__(self, master_data: dict, logger: RequestLogger | None = None):
        super().__init__(logger=logger)
        try:
            # Копируем, чтобы не мутировать исходный dict
            self.master_data = master_data.copy()

            # Обязательные поля (уточните под вашу модель)
            required_fields = {"name", "organization_id"}
            missing = required_fields - self.master_data.keys()
            if missing:
                raise ValueError(f"Отсутствуют обязательные поля: {missing}")

            self.logger.info(
                "Начало создания нового мастера",
                extra={
                    "event": "master.create.request",
                    "organization_id": self.master_data.get("organization_id"),
                    "master_name": self.master_data.get("name"),
                    "has_telegram_id": "telegram_id" in self.master_data,
                },
            )

        except (ValueError, TypeError) as e:
            self.logger.warning(
                "Некорректные данные при создании мастера",
                extra={"event": "master.create.invalid_input", "error": str(e)},
            )
            raise ValidationError(
                {"message": "Неверный формат данных", "success": False},
                code=400,
            )
        except Exception as e:
            self.logger.exception(
                "Неожиданная ошибка при инициализации создания мастера",
                extra={"event": "master.create.init_fail"},
            )
            raise ValidationError(
                {"message": "Внутренняя ошибка сервера", "success": False},
                code=500,
            )

    def create_master(self):
        # Убираем потенциально опасные поля (если передали)
        safe_data = {
            k: v
            for k, v in self.master_data.items()
            if k in [f.name for f in Master._meta.fields]  # только поля модели
        }

        try:
            self.master = Master.objects.create(**safe_data)

            self.logger.info(
                "Мастер успешно создан в БД",
                extra={
                    "event": "master.create.success",  # ← исправлено: было "master.delete.success"
                    "master_id": self.master.pk,
                    "master_name": self.master.name,
                    "organization_id": self.master.organization_id,
                    "code_length": len(str(self.master.code))
                    if self.master.code
                    else 0,
                },
            )

        except IntegrityError as e:
            self.logger.error(
                "Нарушение целостности при создании мастера (дубль, FK и т.п.)",
                extra={
                    "event": "master.create.integrity_fail",
                    "error": str(e),
                    "organization_id": self.master_data.get("organization_id"),
                    "master_name": self.master_data.get("name"),
                },
            )
            raise ValidationError(
                {
                    "message": "Мастер с таким именем уже существует в этой организации",
                    "success": False,
                },
                code=409,
            )
        except Exception as e:
            self.logger.exception(
                "Ошибка при сохранении мастера в БД",
                extra={
                    "event": "master.create.db_fail",
                    "error_type": type(e).__name__,
                },
            )
            raise ValidationError(
                {"message": "Не удалось создать мастера", "success": False},
                code=422,
            )

    def _publish_event(self):
        """Отправка события в Kafka: мастер создан (но ещё не верифицирован)"""
        event_data = {
            "event": "master.created",
            "data": {
                "master_id": self.master.pk,
                "name": self.master.name,
                "organization_id": self.master.organization_id,
                "code": str(self.master.code),  # код для верификации
                "created_at": self.master.created_at.isoformat()
                if hasattr(self.master, "created_at")
                else None,
            },
            "metadata": {
                "request_id": getattr(self.logger, "request_id", "unknown"),
                "source": "bookingmaster-api",
                "service": "masters",
            },
        }

        try:
            self.logger.debug(
                "Постановка события создания мастера в очередь брокера",
                extra={
                    "event": "master.create.event_queued",
                    "master_id": self.master.pk,
                },
            )
            send_message_in_broker.delay(event_data)
            self.logger.info(
                "Событие 'master.created' отправлено в брокер",
                extra={
                    "event": "master.create.event_sent",
                    "master_id": self.master.pk,
                },
            )
        except Exception as e:
            self.logger.error(
                "Не удалось отправить событие создания мастера в брокер",
                extra={
                    "event": "master.create.event_fail",
                    "master_id": self.master.pk,
                    "error": str(e),
                },
            )

    def execute(self):
        self.create_master()
        self._publish_event()  # ← было объявлено, но не вызвано!

        return Response(
            {
                "message": "Мастер успешно создан. Код для верификации в Telegram выдан.",
                "success": True,
                "data": {
                    "code": str(self.master.code),  # ← явно как строка
                    "master_id": self.master.pk,
                    "name": self.master.name,
                },
            },
            status=201,
        )


class MasterEditSrv(BaseService):
    def __init__(
        self, master_id: int, master_data: dict, logger: RequestLogger | None = None
    ):
        super().__init__(logger=logger)
        self.master_data = master_data
        self.master_id = master_id

    def get_master(self):
        self.master = Master.objects.filter(pk=self.master_id).first()
        if not self.master:
            self.logger.warning(
                "Попытка редактирования несуществующего мастера",
                extra={"event": "master.edit.not_found", "master_id": self.master_id},
            )
            raise ValidationError(
                {"message": "Мастер не найден", "success": False}, code=404
            )

    def update_master(self):
        updated = self.master.__class__.objects.filter(pk=self.master.pk).update(
            **self.master_data
        )
        if updated:
            self.logger.info(
                "Мастер успешно обновлён",
                extra={"event": "master.edit.success", "master_id": self.master_id},
            )
        else:
            self.logger.warning(
                "Обновление мастера не привело к изменениям",
                extra={"event": "master.edit.no_change", "master_id": self.master_id},
            )

    def _publish_event(self):
        event_data = {
            "event": "master.updated",
            "data": {"master_id": self.master_id},
            "metadata": {"request_id": getattr(self.logger, "request_id", "unknown")},
        }
        try:
            send_message_in_broker.delay(event_data)
            self.logger.debug(
                "Событие master.updated отправлено", extra={"master_id": self.master_id}
            )
        except Exception as e:
            self.logger.error(
                "Ошибка отправки события master.updated", extra={"error": str(e)}
            )

    def execute(self):
        self.get_master()
        self.update_master()
        self._publish_event()
        return Response(
            {"message": "Мастер изменён", "success": True, "data": []}, status=200
        )


class MasterServiceListSrv(BaseService):
    def __init__(self, *args, logger: RequestLogger | None = None, **kwargs):
        super().__init__(logger=logger)
        self.master_id = kwargs.get("master_id")

    def get_master_services(self):
        self.master = Master.objects.filter(pk=self.master_id).first()
        if not self.master:
            self.logger.warning(
                "Попытка получения услуг несуществующего мастера",
                extra={
                    "event": "master.services.list.not_found",
                    "master_id": self.master_id,
                },
            )
            raise ValidationError(
                {"message": "Мастер не найден", "success": False}, code=404
            )
        self.master_services = self.master.service_set.values(
            "id", "title", "short_description", "price", "min_time", "master_id"
        )
        self.logger.debug(
            "Получен список услуг мастера",
            extra={
                "event": "master.services.list.success",
                "master_id": self.master_id,
                "count": len(self.master_services),
            },
        )

    def execute(self):
        self.get_master_services()
        return Response(
            {
                "message": "Запрос прошёл успешно",
                "success": True,
                "data": self.master_services,
            },
            status=200,
        )


class MasterServiceCreateSrv(BaseService):
    def __init__(self, *args, logger: RequestLogger | None = None, **kwargs):
        super().__init__(logger=logger)
        self.master_id = kwargs.get("master_id")
        self.service_data = kwargs.get("service_data")

    def get_master(self):
        self.master = Master.objects.filter(pk=self.master_id).first()
        if not self.master:
            self.logger.warning(
                "Попытка создания услуги для несуществующего мастера",
                extra={
                    "event": "service.create.master_not_found",
                    "master_id": self.master_id,
                },
            )
            raise ValidationError({"message": "Нет такого мастера", "success": False})

    def create_service(self):
        self.service = self.master.service_set.create(**self.service_data)
        self.logger.info(
            "Услуга успешно создана",
            extra={
                "event": "service.create.success",
                "service_id": self.service.pk,
                "master_id": self.master_id,
            },
        )

    def _publish_event(self):
        event_data = {
            "event": "service.created",
            "data": {"service_id": self.service.pk, "master_id": self.master_id},
            "metadata": {"request_id": getattr(self.logger, "request_id", "unknown")},
        }
        try:
            send_message_in_broker.delay(event_data)
            self.logger.debug(
                "Событие service.created отправлено",
                extra={"service_id": self.service.pk},
            )
        except Exception as e:
            self.logger.error(
                "Ошибка отправки события service.created", extra={"error": str(e)}
            )

    def execute(self):
        self.get_master()
        self.create_service()
        self._publish_event()
        return Response(
            {
                "message": "Запрос прошёл успешно",
                "success": True,
                "data": {},
                "service_id": self.service.pk,
            },
            status=201,
        )


class MasterServiceEditSrv(BaseService):
    def __init__(self, *args, logger: RequestLogger | None = None, **kwargs):
        super().__init__(logger=logger)
        self.service_id = kwargs.get("service_id")
        self.service_data = kwargs.get("service_data")

    def get_and_update_service(self):
        updated = Service.objects.filter(pk=self.service_id).update(**self.service_data)
        if not updated:
            self.logger.warning(
                "Попытка обновления несуществующей услуги",
                extra={
                    "event": "service.edit.not_found",
                    "service_id": self.service_id,
                },
            )
            raise ValidationError(
                {"message": "Услуга не найдена", "success": False}, code=404
            )
        self.logger.info(
            "Услуга успешно обновлена",
            extra={"event": "service.edit.success", "service_id": self.service_id},
        )

    def _publish_event(self):
        event_data = {
            "event": "service.updated",
            "data": {"service_id": self.service_id},
            "metadata": {"request_id": getattr(self.logger, "request_id", "unknown")},
        }
        try:
            send_message_in_broker.delay(event_data)
            self.logger.debug(
                "Событие service.updated отправлено",
                extra={"service_id": self.service_id},
            )
        except Exception as e:
            self.logger.error(
                "Ошибка отправки события service.updated", extra={"error": str(e)}
            )

    def execute(self):
        self.get_and_update_service()
        self._publish_event()
        return Response(
            {"message": "Запрос прошёл успешно", "success": True, "data": {}},
            status=200,
        )


class MasterServiceDeleteSrv(BaseService):
    def __init__(self, *args, logger: RequestLogger | None = None, **kwargs):
        super().__init__(logger=logger)
        self.service_id = kwargs.get("service_id")

    def get_and_delete_service(self):
        deleted, _ = Service.objects.filter(pk=self.service_id).delete()
        if not deleted:
            self.logger.warning(
                "Попытка удаления несуществующей услуги",
                extra={
                    "event": "service.delete.not_found",
                    "service_id": self.service_id,
                },
            )
            raise ValidationError(
                {"message": "Услуга не найдена", "success": False}, code=404
            )
        self.logger.info(
            "Услуга успешно удалена",
            extra={"event": "service.delete.success", "service_id": self.service_id},
        )

    def _publish_event(self):
        event_data = {
            "event": "service.deleted",
            "data": {"service_id": self.service_id},
            "metadata": {"request_id": getattr(self.logger, "request_id", "unknown")},
        }
        try:
            send_message_in_broker.delay(event_data)
            self.logger.debug(
                "Событие service.deleted отправлено",
                extra={"service_id": self.service_id},
            )
        except Exception as e:
            self.logger.error(
                "Ошибка отправки события service.deleted", extra={"error": str(e)}
            )

    def execute(self):
        self.get_and_delete_service()
        self._publish_event()
        return Response(
            {"message": "Запрос прошёл успешно", "success": True, "data": {}},
            status=204,
        )


class MasterServiceDetailSrv(BaseService):
    def __init__(self, *args, logger: RequestLogger | None = None, **kwargs):
        super().__init__(logger=logger)
        self.service_id = kwargs.get("service_id")

    def get_master_service_detail(self):
        self.services = Service.objects.filter(pk=self.service_id)
        if not self.services.exists():
            self.logger.warning(
                "Попытка получения несуществующей услуги",
                extra={
                    "event": "service.detail.not_found",
                    "service_id": self.service_id,
                },
            )
            raise ValidationError(
                {"message": "Такой услуги нет в системе", "success": False}
            )
        self.service = self.services.values(
            "id", "title", "short_description", "price", "min_time", "master_id"
        ).first()
        self.logger.debug(
            "Детали услуги получены",
            extra={"event": "service.detail.success", "service_id": self.service_id},
        )

    def execute(self):
        self.get_master_service_detail()
        return Response(
            {"message": "Запрос прошёл успешно", "success": True, "data": self.service}
        )


class CustomerListSrv(BaseService):
    def __init__(
        self, organization_telegram_id: int, logger: RequestLogger | None = None
    ):
        super().__init__(logger=logger)
        self.organization_telegram_id = organization_telegram_id

    def get_organization_customers(self):
        # Исправлено: было 'organizatoin' → 'organization'
        self.customers = Customer.objects.filter(
            organization__telegram_id=self.organization_telegram_id
        )
        self.logger.debug(
            "Получен список клиентов организации",
            extra={
                "event": "customer.list.success",
                "organization_telegram_id": self.organization_telegram_id,
                "count": self.customers.count(),
            },
        )

    def execute(self):
        self.get_organization_customers()
        return Response(
            {
                "message": "Запрос прошёл успешно",
                "success": True,
                "data": list(self.customers.values()),
            }
        )


class MasterVerifySrv(BaseService):
    def __init__(
        self, code: str, telegram_id: str, logger: RequestLogger | None = None
    ):
        super().__init__(logger=logger)
        self.code = code
        self.telegram_id = telegram_id

    def get_master_by_code(self):
        self.master = Master.objects.filter(code=self.code).first()
        if not self.master:
            self.logger.warning(
                "Мастер не найден по коду",
                extra={"event": "master.verify.not_found", "code": self.code},
            )

    def check_master(self):
        if not self.master:
            raise ValidationError(
                {
                    "message": "Такого пользователя нет в системе. Попробуйте ещё раз",
                    "success": False,
                }
            )
        if self.master.is_verified:
            self.logger.info(
                "Попытка повторной верификации мастера",
                extra={"event": "master.verify.already", "master_id": self.master.pk},
            )
            raise ValidationError(
                {
                    "message": "Такой пользователь уже есть в системе",
                    "success": True,
                }
            )
        self.master.telegram_id = self.telegram_id
        self.master.is_verified = True
        self.master.save()
        self.logger.info(
            "Мастер успешно верифицирован",
            extra={
                "event": "master.verify.success",
                "master_id": self.master.pk,
                "telegram_id": self.telegram_id,
            },
        )

    def send_notification(self):
        if self.master:
            message_data = {
                "event": "master.verify",
                "notification_type": "telegram",
                "data": {
                    "master_name": self.master.name,
                    "master_surname": getattr(self.master, "surname", ""),
                    "organization_telegram_id": getattr(
                        self.master.organization, "telegram_id", ""
                    ),
                },
                "metadata": {
                    "request_id": getattr(self.logger, "request_id", "unknown"),
                    "timestamp": timezone.now().isoformat(),
                    "source": "bookingmaster-api",
                },
                "_from": "bot",
            }
            try:
                send_message_in_broker.delay(message_data)
                self.logger.debug(
                    "Событие master.verify отправлено в брокер",
                    extra={"master_id": self.master.pk},
                )
            except Exception as e:
                self.logger.error(
                    "Ошибка отправки события master.verify", extra={"error": str(e)}
                )
            try:
                send_message_about_verify_master.delay(self.master.id)
                self.logger.debug(
                    "Уведомление о верификации мастера поставлено в очередь",
                    extra={"master_id": self.master.pk},
                )
            except Exception as e:
                self.logger.error(
                    "Ошибка постановки уведомления о верификации",
                    extra={"error": str(e)},
                )

    def _publish_event(self):
        if self.master:
            event_data = {
                "event": "master.telegram_linked",
                "data": {"master_id": self.master.pk, "telegram_id": self.telegram_id},
                "metadata": {
                    "request_id": getattr(self.logger, "request_id", "unknown")
                },
            }
            try:
                send_message_in_broker.delay(event_data)
                self.logger.debug(
                    "Событие master.telegram_linked отправлено",
                    extra={"master_id": self.master.pk},
                )
            except Exception as e:
                self.logger.error(
                    "Ошибка отправки события master.telegram_linked",
                    extra={"error": str(e)},
                )

    def execute(self):
        self.get_master_by_code()
        self.check_master()
        self.send_notification()
        self._publish_event()
        return Response(
            {"message": "Вы авторизованы", "success": True, "data": []}, status=200
        )


class MasterCustomers(BaseService):
    def __init__(self, serializer_data: dict, logger: RequestLogger | None = None):
        super().__init__(logger=logger)
        self.telegram_id = serializer_data.get("telegram_id")

    def get_master(self):
        self.master = Master.objects.filter(telegram_id=self.telegram_id).first()
        if not self.master:
            self.logger.warning(
                "Клиенты запрошены для несуществующего мастера",
                extra={
                    "event": "master.customers.not_found",
                    "telegram_id": self.telegram_id,
                },
            )
            raise ValidationError(
                {
                    "message": "Такого мастера нет в системе",
                    "success": False,
                }
            )

    def get_master_clients(self):
        self.customers = self.master.customer_set.all().values("id", "username")
        self.logger.debug(
            "Получен список клиентов мастера",
            extra={
                "event": "master.customers.success",
                "master_id": self.master.pk,
                "count": len(self.customers),
            },
        )

    def execute(self):
        self.get_master()
        self.get_master_clients()
        return Response(
            {
                "message": "Список клиентов получен",
                "success": True,
                "data": list(self.customers),
            }
        )


class MasterNextSessionSrv(BaseService):
    def __init__(self, serializer_data: dict, logger: RequestLogger | None = None):
        super().__init__(logger=logger)
        self.telegram_id = serializer_data.get("telegram_id")

    def get_master(self):
        self.master = Master.objects.filter(telegram_id=self.telegram_id).first()
        if not self.master:
            self.logger.warning(
                "Следующая сессия запрошена для несуществующего мастера",
                extra={
                    "event": "master.next_session.not_found",
                    "telegram_id": self.telegram_id,
                },
            )
            raise ValidationError(
                {
                    "message": "Такого мастера нет в системе",
                    "success": False,
                }
            )

    def get_master_bookings(self):
        now = datetime.now()
        self.booking = (
            self.master.booking_set.filter(
                booking_date__gte=now.date(),
                booking_time__gte=now.time()
                if now.date() == datetime.min.date()
                else datetime.min.time(),
            )
            .order_by("booking_date", "booking_time")
            .annotate(start_time=F("booking_time"), start_date=F("booking_date"))
            .values("start_time", "start_date")
            .first()
        )
        if not self.booking:
            self.logger.debug(
                "У мастера нет будущих броней",
                extra={
                    "event": "master.next_session.no_bookings",
                    "master_id": self.master.pk,
                },
            )

    def complete_time(self):
        try:
            self.next_time = next_session(**self.booking)
            self.logger.debug(
                "Рассчитано время до следующей сессии",
                extra={
                    "event": "master.next_session.calculated",
                    "master_id": self.master.pk,
                },
            )
        except Exception as e:
            self.logger.error(
                "Ошибка расчёта времени до следующей сессии",
                extra={"error": str(e), "master_id": self.master.pk},
            )
            self.next_time = "Ошибка расчёта времени"

    def execute(self):
        self.get_master()
        self.get_master_bookings()
        if not self.booking:
            return Response(
                {"message": "У вас нет броней", "success": True, "data": []}
            )
        self.complete_time()
        return Response({"message": self.next_time, "success": True, "data": []})


class CustomerNextSessionSrv(BaseService):
    def __init__(self, serializer_data: dict, logger: RequestLogger | None = None):
        super().__init__(logger=logger)
        self.telegram_id = serializer_data.get("telegram_id")

    def get_client(self):
        self.customer = Customer.objects.filter(telegram_id=self.telegram_id).first()
        if not self.customer:
            self.logger.warning(
                "Следующая сессия запрошена для несуществующего клиента",
                extra={
                    "event": "customer.next_session.not_found",
                    "telegram_id": self.telegram_id,
                },
            )
            raise ValidationError(
                {
                    "message": "Такого клиента нет в системе",
                    "success": False,
                }
            )

    def get_customer_bookings(self):
        now = datetime.now()
        self.booking = (
            self.customer.master.booking_set.filter(
                booking_date__gte=now.date(),
                booking_time__gte=now.time()
                if now.date() == datetime.min.date()
                else datetime.min.time(),
            )
            .order_by("booking_date", "booking_time")
            .annotate(start_time=F("booking_time"), start_date=F("booking_date"))
            .values("start_time", "start_date")
            .first()
        )
        if not self.booking:
            self.logger.debug(
                "У клиента нет будущих броней",
                extra={
                    "event": "customer.next_session.no_bookings",
                    "customer_id": self.customer.pk,
                },
            )

    def complete_time(self):
        try:
            self.next_time = next_session(**self.booking)
            self.logger.debug(
                "Рассчитано время до следующей сессии клиента",
                extra={
                    "event": "customer.next_session.calculated",
                    "customer_id": self.customer.pk,
                },
            )
        except Exception as e:
            self.logger.error(
                "Ошибка расчёта времени до следующей сессии клиента",
                extra={"error": str(e), "customer_id": self.customer.pk},
            )
            self.next_time = "Ошибка расчёта времени"

    def execute(self):
        self.get_client()
        self.get_customer_bookings()
        if not self.booking:
            return Response(
                {"message": "У вас нет броней", "success": True, "data": []}
            )
        self.complete_time()
        return Response({"message": self.next_time, "success": True, "data": []})


class CustomerVerifySrv(BaseService):
    def __init__(self, serializer_data: dict, logger: RequestLogger | None = None):
        super().__init__(logger=logger)
        self.code = serializer_data.get("code")
        self.telegram_id = serializer_data.get("telegram_id")
        self.username = serializer_data.get("username")

    def get_customer_by_code(self):
        self.customer = Customer.objects.filter(code=self.code).first()
        if not self.customer:
            self.logger.warning(
                "Клиент не найден по коду",
                extra={"event": "customer.verify.not_found", "code": self.code},
            )

    def verify_customer(self):
        if not self.customer:
            raise ValidationError(
                {
                    "message": "Такого пользователя нет в системе. Попробуйте ещё раз",
                    "success": False,
                }
            )
        if self.customer.is_verified:
            self.logger.info(
                "Попытка повторной верификации клиента",
                extra={
                    "event": "customer.verify.already",
                    "customer_id": self.customer.pk,
                },
            )
            raise ValidationError(
                {
                    "message": "Такой пользователь уже есть в системе",
                    "success": True,
                }
            )
        self.customer.telegram_id = self.telegram_id
        self.customer.is_verified = True
        self.customer.username = self.username
        self.customer.save()
        self.logger.info(
            "Клиент успешно верифицирован",
            extra={
                "event": "customer.verify.success",
                "customer_id": self.customer.pk,
                "telegram_id": self.telegram_id,
            },
        )

    def send_notification(self):
        if self.customer and self.customer.master:
            try:
                send_message_about_verify_customer.delay(
                    self.customer.master.telegram_id, self.telegram_id
                )
                self.logger.debug(
                    "Уведомление о верификации клиента поставлено в очередь",
                    extra={
                        "customer_id": self.customer.pk,
                        "master_id": self.customer.master.pk,
                    },
                )
            except Exception as e:
                self.logger.error(
                    "Ошибка постановки уведомления о верификации клиента",
                    extra={"error": str(e)},
                )

    def _publish_event(self):
        if self.customer:
            event_data = {
                "event": "customer.telegram_linked",
                "data": {
                    "customer_id": self.customer.pk,
                    "telegram_id": self.telegram_id,
                },
                "metadata": {
                    "request_id": getattr(self.logger, "request_id", "unknown")
                },
            }
            try:
                send_message_in_broker.delay(event_data)
                self.logger.debug(
                    "Событие customer.telegram_linked отправлено",
                    extra={"customer_id": self.customer.pk},
                )
            except Exception as e:
                self.logger.error(
                    "Ошибка отправки события customer.telegram_linked",
                    extra={"error": str(e)},
                )

    def execute(self):
        self.get_customer_by_code()
        self.verify_customer()
        self.send_notification()
        self._publish_event()
        return Response(
            {"message": "Вы авторизованы", "success": True, "data": []}, status=200
        )


class CheckCustomerSrv(BaseService):
    def __init__(self, serializer_data: dict, logger: RequestLogger | None = None):
        super().__init__(logger=logger)
        self.telegram_id = serializer_data.get("telegram_id")

    def get_customer_by_code(self):
        self.customer = (
            Customer.objects.filter(telegram_id=self.telegram_id)
            .values("telegram_id", "name", "phone")
            .first()
        )
        if self.customer:
            self.logger.debug(
                "Проверка клиента: аккаунт существует",
                extra={
                    "event": "customer.check.exists",
                    "telegram_id": self.telegram_id,
                },
            )
        else:
            self.logger.debug(
                "Проверка клиента: аккаунт не найден",
                extra={
                    "event": "customer.check.not_found",
                    "telegram_id": self.telegram_id,
                },
            )

    def execute(self):
        self.get_customer_by_code()
        if self.customer:
            return Response({"data": self.customer}, status=200)
        else:
            return Response(status=404)
