# your_app/management/commands/generate_test_data.py
from django.core.management.base import BaseCommand
from django.utils import timezone
from src.models import (
    OrganizationType,
    Organization,
    Master,
    Service,
    Customer,
    Order,
    Booking,
    Image,
    Moderator,
)
from src.enums import statuses
import random
import string
from datetime import date, time, timedelta


def random_string(length=8):
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


class Command(BaseCommand):
    help = "Генерирует тестовые данные для моделей приложения"

    def handle(self, *args, **kwargs):
        self.stdout.write("Начинаем генерацию тестовых данных...")

        # 1. OrganizationType
        org_types = []
        for i in range(3):
            ot, _ = OrganizationType.objects.get_or_create(title=f"Тип {i + 1}")
            org_types.append(ot)

        # 2. Organization
        orgs = []
        for i in range(5):
            org = Organization.objects.create(
                telegram_id=str(100000 + i),
                title=f"Салон {i + 1}",
                address=f"Адрес {i + 1}",
                contact_phone=f"+791234567{i}0",
                time_begin=time(9, 0),
                time_end=time(20, 0),
                work_schedule="Пн-Вс 9:00-20:00",
                organization_type=random.choice(org_types),
                is_verified=random.choice([True, False]),
            )
            orgs.append(org)

        # 3. Master
        masters = []
        for i in range(10):
            org = random.choice(orgs)
            master = Master.objects.create(
                telegram_id=str(200000 + i),
                name=f"Имя{i + 1}",
                surname=f"Фамилия{i + 1}",
                gender=random.choice(
                    [statuses.CHOICES_GENDER[0][0], statuses.CHOICES_GENDER[1][0]]
                ),
                organization=org,
                is_verified=random.choice([True, False]),
            )
            masters.append(master)

        # 4. Service
        services = []
        for master in masters:
            for j in range(random.randint(1, 4)):
                service = Service.objects.create(
                    master=master,
                    title=f"Услуга {j + 1} от {master.name}",
                    short_description=f"Описание услуги {j + 1}",
                    price=random.randint(500, 5000),
                    min_time=random.randint(30, 120),
                )
                services.append(service)

        # 5. Customer
        customers = []
        for i in range(15):
            customer = Customer.objects.create(
                telegram_id=str(300000 + i),
                phone=f"+79000000{i:03d}",
                username=f"user_{i}",
                name=f"Клиент {i}",
                is_verified=random.choice([True, False]),
            )
            customers.append(customer)

        # 6. Order
        for i in range(20):
            customer = random.choice(customers)
            order = Order.objects.create(
                begin_date=date.today() + timedelta(days=random.randint(-10, 10)),
                begin_time=time(random.randint(9, 19), random.choice([0, 15, 30, 45])),
                customer=customer if random.random() > 0.2 else None,
                customer_phone=customer.phone if customer else f"+790011122{i:02d}",
                customer_name=customer.name if customer else f"Имя {i}",
                customer_notice="Тестовый комментарий"
                if random.random() > 0.5
                else None,
                status=random.choice([choice[0] for choice in statuses.CHOICES_STATUS]),
            )
            # Связываем случайные услуги (от 1 до 3)
            selected_services = random.sample(services, k=min(3, len(services)))
            order.services.set(selected_services)

            # Рассчитываем length_time как сумму min_time услуг
            total_time = sum(s.min_time for s in selected_services)
            order.length_time = total_time
            order.save()

        # 7. Booking
        for i in range(25):
            master = random.choice(masters)
            customer = random.choice(customers) if random.random() > 0.3 else None
            start_time = time(random.randint(9, 18), random.choice([0, 15, 30, 45]))
            duration = random.randint(30, 120)
            # Простое приближение: end_time = start_time + duration (в минутах)
            total_minutes = start_time.hour * 60 + start_time.minute + duration
            end_hour = total_minutes // 60
            end_min = total_minutes % 60
            end_time = time(end_hour % 24, end_min)

            Booking.objects.create(
                booking_date=date.today() + timedelta(days=random.randint(-5, 15)),
                booking_time=start_time,
                booking_end_time=end_time,
                master=master,
                customer=customer,
            )

        # 8. Image
        for org in orgs[:3]:  # Только для первых 3 организаций
            Image.objects.create(
                organization=org,
                image_url=f"https://example.com/images/{random_string()}.jpg",
                priority=random.randint(1, 10),
            )

        # 9. Moderator
        for i in range(3):
            Moderator.objects.create(
                telegram_id=str(400000 + i), login=f"moderator_{i}"
            )

        self.stdout.write(self.style.SUCCESS("Тестовые данные успешно созданы!"))
