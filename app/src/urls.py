"""
Main url models
"""

from django.urls import path
from django.shortcuts import render

from src.views.order_views import OrderCreateView, FreeBookingView
from src.views.organization_views import (
    OrganizationListView,
    OrganizationDetailView,
    OrganizationTypeListView,
    SearchOrganization,
    ServiceListView,
    MasterListView,
)


urlpatterns = [
    path("order/create/", OrderCreateView.as_view()),
    path("booking/get-free-times/", FreeBookingView.as_view()),
    path("organizations/", OrganizationListView.as_view()),
    path("organizations/<int:organization_id>/", OrganizationDetailView.as_view()),
    path("organization/search/", SearchOrganization.as_view()),
    path("organizations-types/", OrganizationTypeListView.as_view()),
    path("services/", ServiceListView.as_view()),
    path("master/services/", MasterListView.as_view()),
]
