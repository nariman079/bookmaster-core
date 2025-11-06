# pylint: disable=missing-class-docstring,too-few-public-methods, invalid-str-returned
"""
Models for app Organizations
"""

import secrets
import uuid

from django.db import models
from src.enums import statuses


class OrganizationType(models.Model):
    """Типы орзанизаций"""

    class Meta:
        verbose_name = "Тип организации"
        verbose_name_plural = "Типы организаций"

    title = models.CharField(verbose_name="Типы оргинизаций", max_length=255)

    def __str__(self):
        return self.title


class Organization(models.Model):
    """
    Модель "Организация"
    """

    class Meta:
        verbose_name = "Салон"
        verbose_name_plural = "Салоны"

    telegram_id = models.CharField("ID Телеграм", max_length=30, editable=False)
    title = models.CharField(
        "Название",
        max_length=30,
    )
    main_image = models.ImageField(
        "Заглавное изображение", upload_to="business", null=True, blank=True
    )
    main_image_url = models.CharField(max_length=400, null=True, blank=True)
    address = models.CharField(
        "Адрес",
        max_length=30,
    )
    contact_phone = models.CharField(
        "Номер телефона",
        max_length=30,
    )
    time_begin = models.TimeField(
        "Начало рабочего дня",
    )
    time_end = models.TimeField(
        "Конец рабочего дня",
    )
    work_schedule = models.CharField("График работы", max_length=30)
    organization_type = models.ForeignKey(
        "src.OrganizationType", on_delete=models.PROTECT
    )
    is_verified = models.BooleanField("Верифицирован", default=False)
    create_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.title


class Master(models.Model):
    """
    Модель "Мастер"
    """

    class Meta:
        verbose_name = "Мастер"
        verbose_name_plural = "Мастера"

    telegram_id = models.CharField(
        "ID Телеграм",
        max_length=30,
        default="web_user",
    )
    code = models.CharField(
        "Код для регистрации", max_length=200, null=True, blank=True
    )
    name = models.CharField(
        "Имя",
        max_length=30,
    )
    surname = models.CharField(
        "Фамилия",
        max_length=30,
    )
    image = models.ImageField("Изображние", upload_to="master", null=True, blank=True)
    image_url = models.CharField(
        "Ссылка на изображение", max_length=255, null=True, blank=True
    )
    gender = models.CharField(
        "Пол", max_length=30, choices=statuses.CHOICES_GENDER, default="MEN", blank=True
    )
    organization = models.ForeignKey(
        Organization, on_delete=models.CASCADE, verbose_name="Организация"
    )
    is_verified = models.BooleanField("Верифицирован", default=False)
    create_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    def save(self, *args, **kwargs):
        if self.pk:
            pass
        else:
            self.code = str(uuid.uuid4())
        return super().save(*args, **kwargs)


class Service(models.Model):
    """
    Модель "Услуга мастера"
    """

    class Meta:
        verbose_name = "Услуга"
        verbose_name_plural = "Услуги"

    master = models.ForeignKey(
        "src.Master",
        on_delete=models.CASCADE,
    )
    title = models.CharField("Название", max_length=30)
    short_description = models.CharField(
        "Короткое описание", max_length=150, null=True, blank=True
    )
    price = models.PositiveIntegerField(
        "Стоимость",
    )
    min_time = models.IntegerField(
        "Минимальная длительность процедуры",
    )
    create_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.title


class Customer(models.Model):
    """
    Модель "Клиент"
    """

    class Meta:
        verbose_name = "Клиент"
        verbose_name_plural = "Клиенты"

    telegram_id = models.CharField("ID Телеграм", max_length=30, default="default-user")
    phone = models.CharField(
        "Номер телефона", max_length=30, null=True, blank=True, unique=True
    )
    username = models.CharField(
        "Имя пользователя telegram", max_length=30, null=True, blank=True
    )
    name = models.CharField("Имя пользователя", max_length=250, null=True, blank=True)
    additional_info = models.CharField(
        "Дополнительная информация", max_length=255, null=True, blank=True
    )
    code = models.CharField(
        "Код для регистрации", max_length=255, null=True, blank=True
    )
    master = models.ForeignKey(
        "src.Master", on_delete=models.SET_NULL, null=True, blank=True
    )
    is_verified = models.BooleanField("Верифицированный клиент", default=False)
    create_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.username or self.name

    def save(self, *args, **kwargs):
        if self.pk:
            pass
        else:
            self.code = str(uuid.uuid4())
        super().save(*args, **kwargs)


class Moderator(models.Model):
    class Meta:
        verbose_name = "Модераторы"

    telegram_id = models.CharField("ID Телеграм", max_length=30, default="default-user")
    login = models.CharField("Логин", max_length=30, unique=True)
    code = models.CharField("Код", max_length=30, null=True, blank=True)
    create_at = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        if self.pk:
            pass
        else:
            self.code = str(secrets.randbelow(90000) + 10000)
        return super().save(*args, **kwargs)

    def __str__(self):
        return self.login
