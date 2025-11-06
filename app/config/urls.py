"""
Main urls
"""

from django.conf import settings
from django.conf.urls.static import static
from django.shortcuts import render
from django.urls import include, path
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularRedocView,
    SpectacularSwaggerView,
)


def index(request):
    return render(request, "index.html")


from src.admin import content_management_admin
from src.views.csm_metrics_views import metrics_view

urlpatterns = [
    path("", index),
    path("admin/", content_management_admin.urls),
    path("api/", include("src.urls")),
    path("bot-api/", include("bot.urls")),
    path("api/schema/", SpectacularAPIView.as_view(), name="schema"),
    path(
        "schema/", SpectacularSwaggerView.as_view(url_name="schema"), name="swagger-ui"
    ),
    path(
        "api/schema/redoc/",
        SpectacularRedocView.as_view(url_name="schema"),
        name="redoc",
    ),
    path("metrics", metrics_view, name="prometheus-django-metrics"),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
