import csv
import os
from datetime import datetime
import time


from station.models import Station
from station.models import Location
from station.models import Observation

with open('../data/station/mesowest_csv.tbl.20130903T1304', 'r') as csvfile:
    reader = csv.reader(csvfile)
    for line in reader:
        break
        if line[0] == 'primary id':
            pass
        mesonet_id = line[0]
        station_name = line[2]
        print "Saved STATION: %s with name: %s" % (mesonet_id, station_name)        
        station = Station(mesonet_id= mesonet_id, name= station_name)
        station.save()


with open('../data/observation/mesowest.out.20130903T1304', 'r') as csvfile:
    reader = csv.reader(csvfile)
    for line in reader:
        if len(line) > 0 and len(line[0].split()) == 16 and str(line[0]).split()[0] != 'STN':
            station = str(line[0]).split()[0]
            s = str(line[0]).split()[1]
            year = int(s[:4])
            month = int(s[4:-7])
            day = int(s[6:-5])
            hour = int(s[9:-2])
            minute = int(s[11:])
            mnet = str(line[0]).split()[2]
            latitude = str(line[0]).split()[3]
            longitude = str(line[0]).split()[4]
            elevation = str(line[0]).split()[5]
            temperature = str(line[0]).split()[6]
            sknt = str(line[0]).split()[7]
            direction = str(line[0]).split()[8]
            gust = str(line[0]).split()[9]
            pmsl = str(line[0]).split()[10]
            altitude = str(line[0]).split()[11]
            dewpoint = str(line[0]).split()[12]
            relhumidity = str(line[0]).split()[13]
            weather = str(line[0]).split()[14]
            p241 = str(line[0]).split()[15]
            
            station_model = Station.objects.get(mesonet_id= station)
            location_model = Location.objects.filter(mesonet_station= station_model, latitude= latitude, longitude= longitude, elevation= elevation)
            location_model = location_model[0] if location_model else None
            if location_model is None:
                location_model = Location(mesonet_station= station_model, latitude= latitude, longitude= longitude, elevation= elevation)
                location_model.save()
             
            _timestamp = str(time.mktime(datetime(year,month,day,hour,minute,0).timetuple()))
            observation_model = Observation(mesonet_id= station, timestamp= datetime(year,month,day,hour,minute,0), location= location_model, temperature= temperature, sknt= sknt, direction= direction, gust= gust, pmsl= pmsl, dewpoint= dewpoint, weather= weather, relhumidity= relhumidity, p24= p241)
            observation_model.save()

exit()

