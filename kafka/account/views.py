from django.shortcuts import render
from django.http import HttpResponse,JsonResponse

from .models import LocationUpdate
# Create your views here.


def index(request):
    return render(request,'account/index.html')


def maps(request):
    latest_update=LocationUpdate.objects.latest('timestap')
    return JsonResponse({
        'latitude':latest_update.latitude,
        'longitude':latest_update.longitude,
        'timestap':latest_update.timestap,
    })