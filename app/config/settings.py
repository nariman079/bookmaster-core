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


SECRET_KEY = os.getenv("SECRET_KEY", 'test')

DEBUG = True

ALLOWED_HOSTS = ["*"]

ROOT_APPS = ["src", "bot", "storages"]
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

REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "test")
REDIS_HOST = os.getenv("REDIS_HOST", "redis-service")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")

if not REDIS_PASSWORD:
    raise ImportError(f"Not found environment element REDIS_PASSWORD")

REDIS_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/0"

cache = redis.Redis(
    host=REDIS_HOST,
    password=REDIS_PASSWORD
)

def dict_set(name: str, data: dict):
    """Создание dict в redis"""
    cache.set(name, json.dumps(data))


def dict_get(name: str) -> None | dict:
    """Получение dict с redis"""
    if value := cache.get(name):
        return json.loads(value)
    return None


CELERY_BROKER_URL = REDIS_URL
CELERY_TIMEZONE = "Europe/Moscow"

FIXMASTER_CLIENT_BOT_TOKEN = os.getenv("FIXMASTER_CLIENT_BOT_TOKEN", 'test')
FIXMASTER_MASTER_BOT_TOKEN = os.getenv("FIXMASTER_MASTER_BOT_TOKEN", 'test')
FIXMASTER_MODERATOR_BOT_TOKEN = os.getenv("FIXMASTER_MODERATOR_BOT_TOKEN", 'test')
FIXMASTER_ORGANIZATION_BOT_TOKEN = os.getenv("FIXMASTER_ORGANIZATION_BOT_TOKEN", 'test')

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    raise ImportError("Set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")

AWS_S3_REGION_NAME = 'us-east-1'
AWS_S3_ENDPOINT_URL = 'http://minio-api-service:9000'
AWS_S3_CUSTOM_DOMAIN = 's3.72.56.248.210.nip.io'
AWS_S3_SIGNATURE_VERSION = 's3v4'
AWS_QUERYSTRING_AUTH = False  # Без подписи в URL
AWS_S3_SECURE_URLS = False     # Используем http

# Имена бакетов
AWS_STORAGE_BUCKET_NAME = 'production-media'
AWS_STATIC_BUCKET_NAME = 'production-static'

# ✅ РАСКОММЕНТИРУЙТЕ И ИСПРАВЬТЕ URL-адреса
# Важно: не добавляем слеш в начале, так как S3 ожидает bucket/путь

# Настройка STORAGES (Django 4.2+)
STORAGES = {
    "default": {
        "BACKEND": "config.custom_storages.CustomS3MediaStorage",
        "OPTIONS": {
            "bucket_name": AWS_STORAGE_BUCKET_NAME,
            "querystring_auth": AWS_QUERYSTRING_AUTH,  # Явно отключаем подпись
            "location": "",
            "access_key": AWS_ACCESS_KEY_ID,
            "secret_key": AWS_SECRET_ACCESS_KEY,
            "endpoint_url": AWS_S3_ENDPOINT_URL,
            "custom_domain": AWS_S3_CUSTOM_DOMAIN,
            "addressing_style": "auto",
    
        }
    },
    "staticfiles": {
        "BACKEND": "config.custom_storages.CustomS3StaticStorage",
        "OPTIONS": {
            "bucket_name": AWS_STATIC_BUCKET_NAME,
            "querystring_auth": AWS_QUERYSTRING_AUTH,  # Отключаем подпись для статики
            "location": "",
        }
    },
}

# STATIC_ROOT не используется, но Django требует
STATIC_ROOT = None
STATIC_URL = f'http://{AWS_S3_CUSTOM_DOMAIN}/{AWS_STATIC_BUCKET_NAME}/'
MEDIA_URL = f'http://{AWS_S3_CUSTOM_DOMAIN}/{AWS_STORAGE_BUCKET_NAME}/'
