# pylint: disable=no-member
"""
Сервисы для работы с заказами и бронированием.
Содержит бизнес-логику создания заказов и проверки свободного времени.
"""

from datetime import time
from datetime import datetime, timedelta
from typing import Any, OrderedDict

from django.db import transaction
from django.db.models.query import QuerySet
from django.db.models import Sum

from rest_framework.exceptions import ValidationError
from rest_framework.response import Response

from config import csm_metrics

from src.tasks import send_message_telegram_on_master, change_status_order, send_message_in_broker
from src.models import Order, Booking, Service, Customer, Organization
from src.utils.logger import RequestLogger


def complete_totals(services: QuerySet["Service"]) -> Any:
    """Complete full time length"""
    return services.aggregate(
        total_time_length=Sum("min_time"), total_price=Sum("price")
    )


def time_to_int(tm: time) -> int:
    """Time to int"""
    return int(tm.strftime("%H:%M").split(":")[0])


def is_free_time(tm: str, available_times: list[str]):
    """Не занятое время"""
    hour = tm.split(":")[0]
    all_times = map(lambda x: x.split(":")[0], available_times)

    return {"time": tm, "is_free": hour not in all_times}


def is_retroactive_date(date):
    """Дата задним числом"""
    return date < datetime.now().date()


# pylint: disable=too-many-instance-attributes,too-few-public-methods
class OrderCreateSrv:
    """
    Create order service
    """

    def __init__(
        self,
        serializer_validate_data: OrderedDict,
        serialzier_data: OrderedDict,
        logger: RequestLogger,
    ):
        self.logger = logger
        self.serializer_data = serialzier_data
        self.master_id = serializer_validate_data.get("master_id")
        self.service_ids = serializer_validate_data.get("service_ids")  # type: ignore
        self.begin_date = serializer_validate_data.get("begin_date")
        self.begin_time = serializer_validate_data.get("begin_time")
        self.customer_phone = serializer_validate_data.get("customer_phone")
        self.customer_name = serializer_validate_data.get("customer_name")
        self.customer_notice = serializer_validate_data.get("customer_notice")
        self.serivces = None
        self.summed_time = None
        self.summed_num = None
        self.booking = None
        self.customer = None
        self.total_price = None
        self.order = None

    def _validate_booking_master(self) -> None:
        bookings = Booking.objects.filter(
            master_id=self.master_id,
            booking_date=self.begin_date,
            booking_time=self.begin_time,
        )
        if bookings:
            raise ValidationError(
                {
                    "success": False,
                    "message": "Время уже забронировано другим пользователем",
                    "data": {},
                },
                code=400,
            )

    def _get_all_services(self) -> None:
        self.serivces = Service.objects.filter(id__in=self.service_ids)

        if not self.serivces:
            raise ValidationError(
                {"message": "Нет таких услуг в системе", "success": False, "data": []},
                code=422,
            )

    def _complete_full_time_length(self) -> None:
        """Complete full time length"""
        sum_total_data = complete_totals(services=self.serivces)
        self.summed_num = sum_total_data["total_time_length"]
        self.summed_time = (
            datetime.combine(self.begin_date, self.begin_time)
            + timedelta(minutes=sum_total_data["total_time_length"])
        ).time()
        self.total_price = sum_total_data["total_price"]

        if not self.summed_time or not self.total_price:
            raise ValidationError(code=422)

    def _create_order(self) -> None:
        """Create order"""
        self.order = Order.objects.create(
            begin_date=self.begin_date,
            begin_time=self.begin_time,
            length_time=self.summed_num,
            customer_phone=self.customer_phone,
            customer_name=self.customer_name,
            customer_notice=self.customer_notice,
        )
        for i in self.service_ids:  # type: ignore
            self.order.services.add(i)
        self.order.save()
        self.logger.debug(
            "Создание заказа",
            extra={
                "booking_date": self.order.begin_date,
                "booking_time": self.begin_time,
                "booking_end_time": self.summed_time,
                "master_id": self.master_id,
            },
        )

    def _create_booking(self):
        """Create booking"""
        self.booking = Booking.objects.create(
            booking_date=self.order.begin_date,
            booking_time=self.begin_time,
            booking_end_time=self.summed_time,
            master_id=self.master_id,
        )
        self.logger.debug(
            "Создание брони",
            extra={
                "booking_date": self.order.begin_date,
                "booking_time": self.begin_time,
                "booking_end_time": self.summed_time,
                "master_id": self.master_id,
            },
        )

    def _create_customer(self):
        """Создание клиента в БД"""
        self.customer = Customer.objects.filter(phone=self.customer_phone).first()

        if not self.customer:
            self.customer = Customer.objects.create(
                phone=self.customer_phone,
                name=self.customer_name,
                master_id=self.master_id,
            )
            csm_metrics.APP_CUSTOMER_COUNT.inc()
            csm_metrics.APP_CUSTOMER_NEW_COUNTER.inc()
            self.logger.debug(
                "Создание нового клиента",
                extra={
                    "customer_id": self.customer.pk,
                    "customer_name": self.customer_name,
                    "customer_phone": self.customer_phone,
                    "event": "customer.add.new",
                },
            )

        else:
            self.customer.name = self.customer_name
            self.customer.save()
            csm_metrics.APP_CUSTOMER_RETURNING_COUNTER.inc()

            self.logger.debug(
                "Добавление существующего пользователя",
                extra={
                    "customer_id": self.customer.pk,
                    "customer_name": self.customer_name,
                    "customer_phone": self.customer_phone,
                    "event": "customer.add.exists",
                },
            )

        self.booking.customer = self.customer
        self.booking.save()

    def _send_notification_on_master(self):
        """Отправка сообщания мастеру о брони"""
        send_message_telegram_on_master.delay(
            self.master_id,
            self.customer_phone,
            self.begin_date,
            self.begin_time,
            self.logger.request_id,
        )
        
        message_data = {
            "master_id": self.master_id,
            "customer_phone": self.customer_phone,
            "begin_date": self.begin_date,
            "begin_time": self.begin_time,
            "request_id": self.logger.request_id,
        }
        send_message_in_broker.delay(
            message_data
        )
    
    def _send_order_status_check(self):
        """Смена статуса брони или заказа"""
        change_in_progress_time = (
            datetime.combine(self.begin_date, self.begin_time) - datetime.now()
        ).total_seconds()

        change_done_time = (
            datetime.combine(self.begin_date, self.begin_time)
            + timedelta(minutes=30)
            - datetime.now()
        ).total_seconds()

        change_status_order.apply_async(
            (self.order.pk, "in-progress", self.logger.request_id),
            countdown=change_in_progress_time,
        )
        change_status_order.apply_async(
            (self.order.pk, "done", self.logger.request_id), countdown=change_done_time
        )

    @transaction.atomic
    def execute(self):
        """Run commands"""
        self._validate_booking_master()
        self._get_all_services()
        self._complete_full_time_length()

        self._create_order()
        self._create_booking()
        self._create_customer()
        for service in self.serivces:
            csm_metrics.APP_SERVICE_SOLD_COUNTER.labels(
                service_id=service.pk,
                service_name=service.title,
                service_price=service.price,
            ).inc()

        self._send_notification_on_master()
        self._send_order_status_check()

        csm_metrics.APP_ORDERS_TOTAL_COUNTER.inc()
        csm_metrics.APP_ORDERS_NEW_TODAY.inc()
        csm_metrics.APP_ORDER_LENGTH_HISTOGRAM.observe(self.summed_num or 60)

        self.logger.debug(
            "Заказ успешно создан",
            extra={
                "order_id": self.order.pk,
                "customer_id": self.customer.pk,
                "event": "order.create",
            },
        )


        return Response(
            {
                "message": "Заказ успешно создан",
                "success": True,
                "data": self.serializer_data,
                "code": self.customer.code,
            },
            status=201,
        )


class FreeBookingSrv:
    """
    Free booking dates and times
    """

    def __init__(self, serializer_validated_data: OrderedDict) -> None:
        self.date = serializer_validated_data.get("date")
        self.master_id = serializer_validated_data.get("master_id")
        self.available_times = set()
        self.bookings = None
        self.free_times = None
        self.times = None
        self.organization = None

    def _get_master_bookings(self) -> None:
        """
        Get all booking on current date
        """
        self.bookings = Booking.objects.filter(
            master_id=self.master_id, booking_date=self.date
        )

    def _generate_organization_times(self):
        self.organization = Organization.objects.filter(master=self.master_id).first()
        start_time = time_to_int(self.organization.time_begin)
        end_time = time_to_int(self.organization.time_end)
        self.times = [f"{i}:00" for i in range(start_time, end_time)]

    def _get_master_available_time(self):
        """
        Get master available time
        """

        if not self.bookings:
            for i in range(
                time_to_int(self.organization.time_begin),
                time_to_int(datetime.now().time()) + 1,
            ):
                self.available_times.add(f"{i}:00")
        for booking in self.bookings:
            start = time_to_int(booking.booking_time)
            end = time_to_int(booking.booking_end_time)

            if (
                booking.booking_date == datetime.now().date()
                and time_to_int(datetime.now().time()) > start
            ):
                for i in range(
                    time_to_int(booking.master.organization.time_begin),
                    time_to_int(datetime.now().time()) + 1,
                ):
                    self.available_times.add(f"{i}:00")
            else:
                for i in range(start, end + 1):
                    self.available_times.add(f"{i}:00")

    def _generate_all_times(self):
        """
        Generate all times for response
        """
        self.free_times = [
            is_free_time(i, sorted(self.available_times)) for i in self.times
        ]

    def execute(self):
        """
        Run commands
        """

        if is_retroactive_date(self.date):
            self._generate_organization_times()
            self.free_times = [{"time": i, "is_free": False} for i in self.times]
            return Response(
                {
                    "message": "All times has been received",
                    "success": True,
                    "data": self.free_times,
                },
                status=200,
            )

        self._get_master_bookings()
        self._generate_organization_times()
        self._get_master_available_time()
        self._generate_all_times()

        return Response(
            {
                "message": "All times has been received",
                "success": True,
                "data": self.free_times,
            },
            status=200,
        )
