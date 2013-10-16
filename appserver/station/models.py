from django.db import models

class Station(models.Model):
    mesonet_id = models.CharField(max_length=250, primary_key=True)
    name = models.CharField(max_length=250)

class Location(models.Model):
    latitude = models.FloatField()
    longitude = models.FloatField()
    elevation = models.FloatField()
    mesonet_station = models.ForeignKey(Station)

class Observation(models.Model):
    mesonet_id = models.CharField(max_length=25)
    timestamp = models.DateTimeField('timestamp')
    location = models.ForeignKey(Location)
    temperature = models.FloatField()
    sknt = models.FloatField()
    direction = models.FloatField()
    gust = models.FloatField()
    pmsl = models.FloatField()
    dewpoint = models.FloatField()
    relhumidity = models.FloatField()
    weather = models.FloatField()
    p24 = models.FloatField()
    class Meta:
        unique_together = ("mesonet_id", "timestamp")
