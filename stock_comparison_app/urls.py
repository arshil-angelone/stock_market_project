from django.urls import path, include
from .views import generate_excel, comparison_form
from django.conf import settings
from . import views

STATIC_URL = '/static/'

STATICFILES_DIRS = [
    settings.BASE_DIR / "static",
]

urlpatterns = [
    path('', comparison_form, name='comparison_form'),  # URL pattern for the comparison form
    path('generate-excel/', generate_excel, name='generate_excel'),  # URL pattern for generating Excel
    path('detailed_comparison/<token>/<stock_name>/', views.detailed_comparison, name='detailed_comparison'),

]
