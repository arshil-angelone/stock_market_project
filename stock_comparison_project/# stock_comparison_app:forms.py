# stock_comparison_app/forms.py
from django import forms

class StockComparisonForm(forms.Form):
    stock_name = forms.CharField(max_length=100)
    interval = forms.ChoiceField(choices=[('1minute', '1 Minute'), ('30minute', '30 Minutes'), ('day', 'Day')])
    from_date = forms.DateField(widget=forms.DateInput(attrs={'type': 'date'}))
    to_date = forms.DateField(widget=forms.DateInput(attrs={'type': 'date'}))
