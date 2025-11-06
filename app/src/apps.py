"""
App settings
"""

import logging
import os
from django.apps import AppConfig


# pylint: disable=missing-class-docstring
class SrcAppConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "src"
    verbose_name = "СУБ"

    def shutdown(self):
        """Дейставия при завершении приложения"""
        logger = logging.getLogger("src.system")
        logger.info(
            "Приложение остановлено",
            extra={"event": "system.app.stopped", "pid": os.getpid()},
        )
