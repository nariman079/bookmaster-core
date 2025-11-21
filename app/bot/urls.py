from django.urls import path
from bot.views import (
    BotMyProfileView,
    BotOrganizationCreateView,
    BotModeratorGetProfileView,
    BotVerifyOrganizationView,
    BotGetOrganizationByTelegramIdView,
    MasterActionView,
    ServiceActionView,
    MasterVerifyView,
    MasterCustomerView,
    MasterNextSessionView,
    CustomerNextSessionView,
    CustomerVerifyView,
    CheckCustomerView,
)

urlpatterns = [
    path("get-my-profile/", BotMyProfileView.as_view(), name="get-profile"),
    path(
        "organization/create/",
        BotOrganizationCreateView.as_view(),
        name="bot-create-organization",
    ),
    path(
        "organization/verify/<int:organization_id>/",
        BotVerifyOrganizationView.as_view(),
        name="bot-organization-verify",
    ),
    path(
        "organization/get-by-telegram_id/<str:telegram_id>",
        BotGetOrganizationByTelegramIdView.as_view(),
        name="bot-organization-get-by-telegram_id",
    ),
    path(
        "moderator/", BotModeratorGetProfileView.as_view(), name="get-moderator-profile"
    ),
    path("masters/<int:master_id>", MasterActionView.as_view()),
    path("masters/", MasterActionView.as_view()),
    path("masters/<int:master_id>/services/", ServiceActionView.as_view()),
    path("service/", ServiceActionView.as_view()),
    path("master/verify/", MasterVerifyView.as_view()),
    path("master/<str:telegram_id>/customers/", MasterCustomerView.as_view()),
    path("master/<str:telegram_id>/last-booking/", MasterNextSessionView.as_view()),
    path("customer/verify/", CustomerVerifyView.as_view()),
    path("customer/<str:telegram_id>/last-booking/", CustomerNextSessionView.as_view()),
    path("customer/check/", CheckCustomerView.as_view()),
]
