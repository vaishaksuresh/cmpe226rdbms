# run this on `manage.py shell` to get django context

import logging

from station.models import Station
from station.models import Observation
from station.models import Location

l = logging.getLogger('django.db.backends')
l.setLevel(logging.DEBUG)
l.addHandler(logging.StreamHandler())

#Query 1
objects = Observation.objects.filter(temperature__gt=65).all();
l = list(objects)
len(l)

#Query 2
objects = Observation.objects.filter(mesonet_id='HVSTA').all();
l = list(objects)
len(l)

#Query 3
objects = Observation.objects.filter(timestamp__gt='2013-09-01 00:15:00').filter(timestamp__lt='2013-09-03 21:02:00').all();
l = list(objects)
len(l)

#Query 4
objects = Location.objects.filter(longitude__gt=-120, longitude__lt=-76, latitude__gt=37, latitude__lt=40).all()
l = list(objects)
[location.mesonet_station for location in l]
