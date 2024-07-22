from django.contrib import admin
from django.urls import path, include
from stock_comparison_app.views import comparison_form  # Import the view
from django.conf import settings

STATIC_URL = '/static/'

STATICFILES_DIRS = [
    settings.BASE_DIR / "static",
]



urlpatterns = [
    path('', comparison_form, name='home'),  # Define a URL pattern for the home page
    path('admin/', admin.site.urls),
    path('stock-comparison/', include('stock_comparison_app.urls')),  # Include app-specific URLs
]
