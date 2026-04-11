from storages.backends.s3boto3 import S3Boto3Storage
from django.conf import settings

class CustomS3StaticStorage(S3Boto3Storage):
    """Кастомный storage для статики с правильным HTTP URL"""
    
    def __init__(self, *args, **kwargs):
        kwargs['bucket_name'] = settings.AWS_STATIC_BUCKET_NAME
        kwargs['querystring_auth'] = False
        kwargs['location'] = ''
        kwargs['default_acl'] = 'public-read'
        super().__init__(*args, **kwargs)
    
    def url(self, name):
        # Получаем URL от родительского класса
        url = super().url(name)
        
        # Исправляем URL: заменяем на HTTP и убираем лишние части
        # Ожидаемый формат: http://s3.72.56.248.210.nip.io/production-static/путь
        custom_domain = settings.AWS_S3_CUSTOM_DOMAIN
        bucket_name = settings.AWS_STATIC_BUCKET_NAME
        
        # Форсируем правильный URL
        correct_url = f"http://{custom_domain}/{bucket_name}/{name}"
        
        # Убираем возможные дублирующиеся слеши
        from urllib.parse import urljoin
        correct_url = urljoin(correct_url, name)
        
        return correct_url


class CustomS3MediaStorage(S3Boto3Storage):
    """Кастомный storage для медиа"""
    
    def __init__(self, *args, **kwargs):
        kwargs['bucket_name'] = settings.AWS_STORAGE_BUCKET_NAME
        kwargs['querystring_auth'] = False
        kwargs['location'] = ''
        kwargs['default_acl'] = 'public-read'
        super().__init__(*args, **kwargs)
    
    def url(self, name):
        custom_domain = settings.AWS_S3_CUSTOM_DOMAIN
        bucket_name = settings.AWS_STORAGE_BUCKET_NAME
        return f"http://{custom_domain}/{bucket_name}/{name}"