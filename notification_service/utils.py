import logging


# pylint: disable=missing-function-docstring
class RequestLogger:
    """
    Логер для запросов

    генерирует request_id
    """

    def __init__(self, request_id: str = "none") -> None:
        self.request_id = request_id
        self.logger = logging.getLogger("src")

    def _log(self, level, msg, **kwargs):
        kwargs.setdefault("extra", {})["request_id"] = self.request_id
        getattr(self.logger, level)(msg, **kwargs)

    def info(self, msg, **kwargs):
        self._log("info", msg, **kwargs)

    def warning(self, msg, **kwargs):
        self._log("warning", msg, **kwargs)

    def error(self, msg, **kwargs):
        self._log("error", msg, **kwargs)

    def debug(self, msg, **kwargs):
        self._log("debug", msg, **kwargs)

    def critical(self, msg, **kwargs):
        self._log("critical", msg, **kwargs)
