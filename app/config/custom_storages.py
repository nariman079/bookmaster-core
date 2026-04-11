from storages.backends.s3boto3 import S3Boto3Storage
from django.conf import settings

class CustomS3StaticStorage(S3Boto3Storage):
    bucket_name = settings.AWS_STATIC_BUCKET_NAME
    querystring_auth = False
    location = ''  # Важно: оставить пустым
    file_overwrite = False
    
    def url(self, name):
        # Убираем возможные дублирования
        # Если name уже содержит production-static, удаляем его
        if name.startswith(f'{self.bucket_name}/'):
            name = name[len(f'{self.bucket_name}/'):]
        
        # Простой и чистый URL
        return f"http://{settings.AWS_S3_CUSTOM_DOMAIN}/{self.bucket_name}/{name}"

class CustomS3MediaStorage(S3Boto3Storage):
    bucket_name = settings.AWS_STORAGE_BUCKET_NAME
    querystring_auth = False
    location = ''
    file_overwrite = False
    
    def url(self, name):
        return f"http://{settings.AWS_S3_CUSTOM_DOMAIN}/{self.bucket_name}/{name}"