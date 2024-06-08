from django.contrib import admin
from django.urls import path
from transport_layer_api import views
from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi
from drf_spectacular.views import SpectacularAPIView, SpectacularRedocView, SpectacularSwaggerView

schema_view = get_schema_view(
    openapi.Info(
        title="Transport Layer",
        default_version='v1',
        description="API для сегментации сообщений и собрки и перессылки на канальный и прикладной уровни",
    ),
    public=True,
    permission_classes=(permissions.AllowAny,),
)

urlpatterns = [
    path("admin/", admin.site.urls),
    path('transferMessage/', views.transfer_msg),
    path('postSegment/', views.post_segment),
    path('swagger/', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
    path('swagger.yaml', schema_view.without_ui(cache_timeout=0), name='schema-swagger'),
]
