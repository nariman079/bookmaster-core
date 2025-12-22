"""
Django settings
"""

import os
from pathlib import Path
import logging

import json
import redis

BASE_DIR = Path(__file__).resolve().parent.parent

LOGGING_DIR = os.path.join(BASE_DIR, "logs")

if not os.path.exists(LOGGING_DIR):
    os.makedirs(LOGGING_DIR)


SECRET_KEY = os.getenv("SECRET_KEY")

DEBUG = True

ALLOWED_HOSTS = ["*"]

ROOT_APPS = ["src", "bot"]
INSTALLED_APPS = [
    "django_celery_beat",
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "corsheaders",
    "drf_spectacular",
    "drf_spectacular_sidecar",
    "django_prometheus",
    *ROOT_APPS,
]
MIDDLEWARE = [
    "django_prometheus.middleware.PrometheusBeforeMiddleware",
    "config.middlewares.RequestTimingMiddleware",
    "config.middlewares.RequestIDMiddleware",
    "config.middlewares.ErrorHandlingMiddleware",
    "config.middlewares.UncaughtExceptionMiddleware",
    "config.middlewares.SecurityIPRateLimitMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "django_prometheus.middleware.PrometheusAfterMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.getenv("POSTGRES_DB"),
        "USER": os.getenv("POSTGRES_USER"),
        "PASSWORD": os.getenv("POSTGRES_PASSWORD"),
        "HOST": os.getenv("POSTGRES_HOST"),
        "PORT": os.getenv("POSTGRES_PORT"),
    },
}


AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

LANGUAGE_CODE = "ru"

TIME_ZONE = "Europe/Moscow"

USE_I18N = True

USE_L10N = True

USE_TZ = True

STATIC_URL = "/static/"
STATIC_ROOT = os.path.join(BASE_DIR, "static")

MEDIA_URL = "/media/"
MEDIA_ROOT = os.path.join(BASE_DIR, "media")

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

CORS_ALLOW_ALL_ORIGINS = True

REST_FRAMEWORK = {
    "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
    "DEFAULT_AUTHENTICATION_CLASSES": [
        # 'rest_framework.authentication.SessionAuthentication',
        "rest_framework.authentication.BasicAuthentication",
    ],
}
DATE_INPUT_FORMATS = ["%m-%d-%Y"]

SPECTACULAR_SETTINGS = {
    "TITLE": "FixMaster API",
    "DESCRIPTION": "Compact booking of master",
    "VERSION": "1.0.0",
    "SERVE_INCLUDE_SCHEMA": False,
    "SWAGGER_UI_DIST": "SIDECAR",
    "SWAGGER_UI_FAVICON_HREF": "SIDECAR",
    "REDOC_DIST": "SIDECAR",
}


class SafeRequestIDFormatter(logging.Formatter):
    def format(self, record):
        # Добавляем request_id, если его нет
        if not hasattr(record, "request_id"):
            record.request_id = "none"
        return super().format(record)


LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "()": SafeRequestIDFormatter,
            "format": "[{asctime}] {request_id} {levelname} {name} {message}",
            "style": "{",
        },
        "main": {
            "format": "[%(asctime)s] %(levelname)s %(request_id)s %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "json": {
            "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
            "format": "%(asctime)s %(name)s %(levelname)s %(request_id)s %(message)s",
            "reserved_attrs": [
                "asctime",
                "name",
                "levelname",
                "exc_info",
                "exc_text",
                "stack_info",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "pathname",
                "filename",
                "module",
                "funcName",
                "lineno",
                "args",
                "msg",
                "levelno",
            ],
            "rename_fields": {
                "levelname": "level",
                "name": "logger",
            },
        },
        "system": {
            "format": "[%(asctime)s] %(levelname)s [SYSTEM] %(message)s",
        },
    },
    "handlers": {
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
        "file": {
            "level": "DEBUG",
            "()": "logging.handlers.TimedRotatingFileHandler",
            "filename": os.path.join(LOGGING_DIR, "django.log"),
            "formatter": "json",
            "when": "midnight",
            "interval": 1,
            "backupCount": 7,
            "encoding": "utf-8",
        },
        "system_console": {
            "formatter": "system",
            "level": "DEBUG",
            "class": "logging.StreamHandler",
        },
    },
    "loggers": {
        "src": {
            "handlers": ["console", "file"],
            "level": "DEBUG",
        },
        "src.errors": {
            "handlers": ["console", "file"],
            "level": "ERROR",
            "propagate": False,
        },
        "src.celery": {
            "handlers": ["console", "file"],
            "level": "DEBUG",
            "propagate": False,
        },
        "src.system": {
            "handlers": ["system_console"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}


cache = redis.Redis(
    host="redis",
)


def dict_set(name: str, data: dict):
    """Создание dict в redis"""
    cache.set(name, json.dumps(data))


def dict_get(name: str) -> None | dict:
    """Получение dict с redis"""
    if value := cache.get(name):
        return json.loads(value)
    return None


CELERY_BROKER_URL = "redis://redis:6379"
CELERY_TIMEZONE = "Europe/Moscow"

FIXMASTER_CLIENT_BOT_TOKEN = os.getenv("FIXMASTER_CLIENT_BOT_TOKEN")
FIXMASTER_MASTER_BOT_TOKEN = os.getenv("FIXMASTER_MASTER_BOT_TOKEN")
FIXMASTER_MODERATOR_BOT_TOKEN = os.getenv("FIXMASTER_MODERATOR_BOT_TOKEN")
FIXMASTER_ORGANIZATION_BOT_TOKEN = os.getenv("FIXMASTER_ORGANIZATION_BOT_TOKEN")
