FROM python:3.11
ENV PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir poetry==1.7.1
WORKDIR /app/
COPY app/poetry.lock app/pyproject.toml /app/
RUN poetry config virtualenvs.create false && poetry install --no-root


COPY app/ /app/
