from django.db import models

class Station(models.Model):
    latitude = models.FloatField()
    longitude = models.FloatField()
    altitude = models.FloatField()
    mesonet_id = models.CharField(max_length=25)
    name = models.CharField(max_length=50)

class Observation(models.Model):
    station = models.ForeignKey(Station)
    timestamp = models.DateTimeField('timestamp')
    temperature = models.FloatField()
    sknt = models.FloatField()
    direction = models.FloatField()
    gust = models.FloatField()
    pmsl = models.FloatField()
    dewpoint = models.FloatField()
    relhumidity = models.FloatField()
    weather = models.FloatField()
    p24 = models.FloatField()

