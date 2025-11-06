from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import OpenApiParameter, OpenApiRequest, extend_schema
from rest_framework.response import Response
from rest_framework.views import APIView

from bot.permissions import api_key_permission
from bot.services import (
    GetProfile,
    BotOrganizationCreate,
    BotModeratorGetProfile,
    BotVerifyOrganization,
    BotGetOrganizationByTelegramId,
    BotGetOrganizationDataByTelegramId,
    MasterDeleteSrv,
    MasterCreateSrv,
    MasterEditSrv,
    MasterServiceCreateSrv,
    MasterServiceDeleteSrv,
    MasterServiceDetailSrv,
    MasterServiceListSrv,
    MasterServiceEditSrv,
    MasterVerifySrv,
    MasterCustomers,
    MasterNextSessionSrv,
    CustomerNextSessionSrv,
    CustomerVerifySrv,
    CheckCustomerSrv,
)
from bot.serializers import (
    BotOrganizationCreateSerializer,
    BotModeratorGetProfileSerializer,
    MasterCreateSerializer,
    MasterEditSerializer,
    MasterServiceCreateSerializer,
    MasterVerifySerializer,
)


class BotMyProfileView(APIView):
    @extend_schema(
        description="Search organization",
        methods=[
            "GET",
        ],
        parameters=[
            OpenApiParameter(
                name="Api-Key",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.HEADER,
                description="Api key for telegram",
            ),
            OpenApiParameter(
                name="phone_number",
                type=OpenApiTypes.STR,
            ),
            OpenApiParameter(name="telegram_id", type=OpenApiTypes.STR),
            OpenApiParameter(name="username", type=OpenApiTypes.STR),
            OpenApiParameter(name="user_keyword", type=OpenApiTypes.STR),
        ],
    )
    def get(self, *args, **kwargs) -> Response:
        """Получение профиля клиента"""
        if api_key_permission(request=self.request):
            user_params = self.request.query_params

            get_profile = GetProfile(data=user_params, logger=self.request.logger)
            return get_profile.execute()

        return Response(
            {
                "message": "Неизвествная ошибка\nОбратитесь к администратору @nariman079i",
                "success": False,
                "data": [],
            },
            status=422,
        )


class BotOrganizationCreateView(APIView):
    @extend_schema(
        description="Search organization",
        methods=[
            "POST",
        ],
        parameters=[
            OpenApiParameter(
                name="Api-Key",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.HEADER,
                description="Api key for telegram",
            ),
            OpenApiParameter(
                name="title",
                type=OpenApiTypes.STR,
            ),
            OpenApiParameter(name="telegram_id", type=OpenApiTypes.STR),
            OpenApiParameter(name="main_image", type=OpenApiTypes.BINARY),
            OpenApiParameter(name="time_begin", type=OpenApiTypes.TIME),
            OpenApiParameter(name="time_end", type=OpenApiTypes.TIME),
            OpenApiParameter(name="address", type=OpenApiTypes.STR),
            OpenApiParameter(name="work_schedule", type=OpenApiTypes.STR),
            OpenApiParameter(name="organization_type", type=OpenApiTypes.STR),
            OpenApiParameter(name="contact_phone", type=OpenApiTypes.STR),
        ],
    )
    def post(self, request, *args, **kwargs) -> Response:
        """Создание организации"""
        if api_key_permission(request=self.request):
            organization_data = BotOrganizationCreateSerializer(data=self.request.data)
            organization_data.is_valid(raise_exception=True)
            create_organization = BotOrganizationCreate(
                organization_data=organization_data.validated_data,
                logger=request.logger,
            )
            return create_organization.execute()

        return Response(
            {
                "message": "Неизвествная ошибка\nОбратитесь к администратору @nariman079i",
                "success": False,
                "data": ["Неверный API-KEY"],
            },
            status=422,
        )


class BotModeratorGetProfileView(APIView):
    def post(self, request, *args, **kwargs) -> Response:
        """Создание организации"""
        if api_key_permission(request=self.request):
            moderator_data = BotModeratorGetProfileSerializer(data=self.request.data)
            moderator_data.is_valid(raise_exception=True)
            moderator_get_profile = BotModeratorGetProfile(
                moderator_data=moderator_data.validated_data, logger=request.logger
            )
            return moderator_get_profile.execute()

        return Response(
            {
                "message": "Неизвествная ошибка\nОбратитесь к администратору @nariman079i",
                "success": False,
                "data": ["Неверный API-KEY"],
            },
            status=422,
        )


class BotVerifyOrganizationView(APIView):
    def post(self, request, *args, **kwargs):
        if api_key_permission(request=self.request):
            verify_organization_data = dict(
                organization_id=kwargs.get("organization_id"),
                is_verify=self.request.data.get("is_verify"),
            )
            verify_organization = BotVerifyOrganization(
                verify_organization_data=verify_organization_data
            )
            return verify_organization.execute()

        return Response(
            {
                "message": "Неизвествная ошибка\nОбратитесь к администратору @nariman079i",
                "success": False,
                "data": ["Неверный API-KEY"],
            },
            status=422,
        )


class BotGetOrganizationByTelegramIdView(APIView):
    def get(self, *args, **kwargs):
        if api_key_permission(request=self.request):
            organization_data = dict(telegram_id=kwargs.get("telegram_id"))

            get_organization_by_telegram_id = BotGetOrganizationByTelegramId(
                organization_data=organization_data
            )
            return get_organization_by_telegram_id.execute()

        return Response(
            {
                "message": "Неизвествная ошибка\nОбратитесь к администратору @nariman079i",
                "success": False,
                "data": ["Неверный API-KEY"],
            },
            status=422,
        )


class BotGetOrganizationDataByTelegramIdView(APIView):
    def get(self, *args, **kwargs):
        if api_key_permission(request=self.request):
            organization_data = dict(telegram_id=kwargs.get("telegram_id"))
            get_organization_by_telegram_id = BotGetOrganizationDataByTelegramId(
                organization_data=organization_data
            )
            return get_organization_by_telegram_id.execute()

        return Response(
            {
                "message": "Неизвествная ошибка\nОбратитесь к администратору @nariman079i",
                "success": False,
                "data": ["Неверный API-KEY"],
            },
            status=422,
        )


class MasterActionView(APIView):
    def post(self, request, *args, **kwargs):
        if api_key_permission(request=self.request):
            serializer = MasterCreateSerializer(data=self.request.data)
            serializer.is_valid(raise_exception=True)
            create_master = MasterCreateSrv(master_data=serializer.validated_data)
            return create_master.execute()

        return Response(
            {
                "message": "Неизвествная ошибка\nОбратитесь к администратору @nariman079i",
                "success": False,
                "data": ["Неверный API-KEY"],
            },
            status=422,
        )

    def patch(self, request, *args, **kwargs):
        if api_key_permission(request=self.request):
            serializer = MasterEditSerializer(data=self.request.data)
            serializer.is_valid(raise_exception=True)
            create_master = MasterEditSrv(
                master_data=serializer.validated_data, master_id=kwargs.get("master_id")
            )
            return create_master.execute()

        return Response(
            {
                "message": "Неизвествная ошибка\nОбратитесь к администратору @nariman079i",
                "success": False,
                "data": ["Неверный API-KEY"],
            },
            status=422,
        )

    def delete(self, request, *args, **kwargs):
        if api_key_permission(request=self.request):
            master_id = kwargs.get("master_id")
            delete_master = MasterDeleteSrv(master_id=master_id)
            return delete_master.execute()

        return Response(
            {
                "message": "Неизвествная ошибка\nОбратитесь к администратору @nariman079i",
                "success": False,
                "data": ["Неверный API-KEY"],
            },
            status=422,
        )


class ServiceActionView(APIView):
    def get(self, request, *args, **kwargs):
        if api_key_permission(self.request):
            service_id = self.request.query_params.get("service_id")
            master_id = kwargs.get("master_id")
            if service_id:
                master_service_detail = MasterServiceDetailSrv(service_id=service_id)
                return master_service_detail.execute()
            else:
                master_service_list = MasterServiceListSrv(master_id=master_id)
                return master_service_list.execute()
        return Response({"message": "Api-Key error", "success": False, "data": []})

    def post(self, request, *args, **kwargs):
        if api_key_permission(self.request):
            master_id = kwargs.get("master_id", None)
            serializer = MasterServiceCreateSerializer(data=self.request.data)
            serializer.is_valid(raise_exception=True)
            master_service_create = MasterServiceCreateSrv(
                master_id=master_id, service_data=serializer.validated_data
            )
            return master_service_create.execute()
        return Response({"message": "Api-Key error", "success": False, "data": []})

    def patch(self, request, *args, **kwargs):
        if api_key_permission(self.request):
            service_id = self.request.query_params.get("service_id", None)
            master_service_edit = MasterServiceEditSrv(
                service_id=service_id, service_data=self.request.data
            )
            return master_service_edit.execute()
        return Response({"message": "Api-Key error", "success": False, "data": []})

    def delete(self, request, *args, **kwargs):
        if api_key_permission(self.request):
            service_id = self.request.query_params.get("service_id", None)
            if service_id:
                master_service_delete = MasterServiceDeleteSrv(
                    service_id=service_id,
                )
            return master_service_delete.execute()
        return Response({"message": "Api-Key error", "success": False, "data": []})


class MasterVerifyView(APIView):
    @extend_schema(
        description="Verfy master",
        methods=[
            "POST",
        ],
        request=[OpenApiRequest()],
    )
    def post(self, request, *args, **kwargs):
        serializer = MasterVerifySerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        if api_key_permission(self.request):
            master_verify = MasterVerifySrv(**serializer.validated_data)
            return master_verify.execute()
        return Response({"message": "Api-Key error", "success": False, "data": []}, 401)


class MasterCustomerView(APIView):
    def get(self, request, *args, **kwargs):
        telegram_id = self.kwargs.get("telegram_id")

        if api_key_permission(self.request):
            master_customers = MasterCustomers(
                serializer_data={"telegram_id": telegram_id}
            )
            return master_customers.execute()
        return Response({"message": "Api-Key error", "success": False, "data": []}, 401)


class MasterNextSessionView(APIView):
    def get(self, request, *args, **kwargs):
        telegram_id = self.kwargs.get("telegram_id")

        if api_key_permission(self.request):
            master_next_session = MasterNextSessionSrv(
                serializer_data={"telegram_id": telegram_id}
            )
            return master_next_session.execute()
        return Response({"message": "Api-Key error", "success": False, "data": []}, 401)


class CustomerNextSessionView(APIView):
    def get(self, request, *args, **kwargs):
        if api_key_permission(self.request):
            customer_next_session = CustomerNextSessionSrv(serializer_data=self.kwargs)
            return customer_next_session.execute()
        return Response({"message": "Api-Key error", "success": False, "data": []}, 401)


class CustomerVerifyView(APIView):
    def get(self, request, *args, **kwargs):
        if api_key_permission(self.request):
            customer_next_session = CustomerVerifySrv(serializer_data=self.request.data)
            return customer_next_session.execute()
        return Response({"message": "Api-Key error", "success": False, "data": []}, 401)


class CheckCustomerView(APIView):
    def post(self, request, *args, **kwargs):
        customer_next_session = CheckCustomerSrv(
            serializer_data=self.request.query_params
        )
        return customer_next_session.execute()
