# BookMaster Backend

**BookMaster** — это **Telegram-first сервис бронирования**, построенный на **Django + FastAPI + Celery + Redis**.  
Он позволяет удобно управлять бронированиями с помощью 4 Telegram-ботов для разных ролей:

- 👤 **Клиент** — поиск и бронирование услуг  
- 🏢 **Организация** — управление мастерами и услугами  
- 🧑‍🔧 **Мастер** — управление своими записями  
- 👮 **Модератор** — проверка и модерация  

Бэкенд предоставляет REST API, управляет заказами и платежами, а также включает мониторинг через Prometheus, Grafana и Loki.  

---

## 🚀 Возможности

- Система бронирования с организациями, мастерами, услугами и клиентами  
- Интеграция с Telegram для всех ролей  
- Асинхронные уведомления через **Celery + Redis**  
- Метрики Prometheus + графики Grafana + логи Loki  
- CI/CD пайплайны (GitHub Actions и GitLab CI)  
- Полностью контейнеризирован через Docker  

---

## 📂 Структура проекта

```
app/
 ├── src/                # Основное приложение Django
 │   ├── models/         # Модели (Организация, Заказ, Бронирование и т.д.)
 │   ├── views/          # API (Организации, Заказы, Метрики)
 │   ├── tasks/          # Celery-задачи (уведомления, метрики)
 │   ├── utils/          # Логирование и вспомогательные утилиты
 │   └── filters/        # Django фильтры
 ├── manage.py           # Точка входа Django
docker-compose.yml       # Определение сервисов (app, db, redis, celery, nginx, monitoring)
Dockerfile               # Сборка backend
.gitlab-ci.yml           # GitLab CI/CD
.github/workflows/       # GitHub Actions
deploy.yml               # Ansible playbook для деплоя
```

---

## 🛠️ Технологии

- **Backend:** Django, DRF, Celery, Redis  
- **База данных:** PostgreSQL  
- **Кэш и брокер задач:** Redis  
- **Веб-сервер:** Nginx  
- **Мониторинг:** Prometheus, Grafana, Loki, Alertmanager  
- **Контейнеризация:** Docker, Docker Compose, Kubernetes (шаблоны)  
- **CI/CD:** GitHub Actions, GitLab CI, Ansible  

---

## ⚙️ Установка и запуск

### 1. Клонировать репозиторий

```bash
git clone https://github.com/your-username/fixmaster_backend.git
cd fixmaster_backend
```

### 2. Создать `.env` файл

```env
WEB_HOST=you_domain_host
AAIO_API_KEY=you_payment_system_key
SECRET_KEY=django-insecure-you_secret_key
DJANGO_SETTINGS_MODULE=config.settings
TEST_DB=False
FIXMASTER_CLIENT_BOT_TOKEN=TOKEN
FIXMASTER_MASTER_BOT_TOKEN=TOKEN
FIXMASTER_MODERATOR_BOT_TOKEN=TOKEN
FIXMASTER_ORGANIZATION_BOT_TOKEN=TOKEN
POSTGRES_HOST=db
POSTGRES_PORT=5432
POSTGRES_USER=test
POSTGRES_PASSWORD=test
POSTGRES_DB=test

```

### 3. Запуск с Docker

```bash
docker-compose up --build -d
```

### 4. Применение миграций

```bash
docker-compose run migrate
```

### 5. Сборка статики

```bash
docker-compose run collectstatic
```

Бэкенд будет доступен по адресу:  
👉 `http://localhost:8000`

---

## 📊 Мониторинг

- **Prometheus:** `http://localhost:9090`  
- **Grafana:** `http://localhost:3000` (логин/пароль: `admin/admin`)  
- **Loki (логи):** `http://localhost:3100`  
- **Alertmanager:** `http://localhost:9093`  

---

## 🔄 CI/CD

- **GitHub Actions** — пайплайны для develop и production  
- **GitLab CI** — тесты, линтеры, проверки безопасности, билд и деплой  
- **Ansible + Docker** — автоматизация деплоя (`deploy.yml`)  

---

## 🧪 Тестирование

Запуск тестов:

```bash
pytest --cov=app/src
```

Отчёт о покрытии сохраняется в `coverage.xml`.  

---

## 📜 Лицензия

Проект распространяется под лицензией **MIT**.  