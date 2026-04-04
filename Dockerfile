# Этап 1: Сборщик 
FROM python:3.12-slim AS builder 
ENV PYTHONUNBUFFERED=1 \
    POETRY_VERSION=1.8.2 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR="/tmp/poetry_cache"




    
RUN pip install --no-cache-dir poetry==$POETRY_VERSION
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --only main --no-interaction --no-ansi




# Этап 2: Финальный образ
FROM python:3.12-slim AS runtime
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    APP_HOME=/app \
    PATH=/opt/poetry/bin:$PATH

RUN groupadd -r appuser && useradd -r -g appuser -d $APP_HOME -s /sbin/nologin appuser

RUN apt-get update && apt-get install -y --no-install-recommends \
    libffi8 \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*


WORKDIR $APP_HOME
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin


COPY app/ $APP_HOME
COPY pyproject.toml #APP_HOME
RUN chown -R appuser:appuser $APP_HOME

USER appuser
CMD ["/bin/sh", "-c", "uvicorn config.asgi:application --host 0.0.0.0 --port 8000 --workers 4"]
