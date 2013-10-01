import sys
import csv
import os
import psycopg2
import time
import datetime
from psycopg2 import Timestamp

class ExtractData:
    def load_observation_data():
        #station,timestamp,mnet,latitude,longitude,elevation,temperature,sknt,direction,gust,pmsl,altitude,dewpoint,relhumidity,weather,p241 = ([] for i in range(16))
        for dirname, dirnames, filenames in os.walk('./out'):
            if '.git' in dirnames:
                # don't go into any .git directories.
                dirnames.remove('.git')
            for filename in filenames:
                start = time.time()
                with open(os.path.join(dirname, filename), 'r') as csvfile:
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
                            '''q = "insert into station_station (latitude, longitude, altitude,mesonet_id) values ('"+latitude+"','"+longitude+"','"+elevation+"','"+mnet+"','"+station+"')"
                            with open ('queries.sql', 'a') as f: f.write (q+'\n')'''
                            try:
                                conn = psycopg2.connect("host='localhost' dbname=weather user=postgres")
                                cur = conn.cursor()
                                cur.execute("INSERT INTO station_observation(station_id, time_year, mesonet_id, "
                                            "latitude, longitude, elevation,temperature, sknt, wind_direction, gust, "
                                            "pmsl, altimeter, dewpoint,relative_humidity, weather, ptwentyfouri) "
                                            "VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s);",
                                            (station, Timestamp(year,month,day,hour,minute), mnet, latitude, longitude, elevation, temperature,
                                             sknt, direction, gust, pmsl, altitude, dewpoint, relhumidity,
                                             weather, p241))
                            except psycopg2.IntegrityError:
                                print "Integrity Exception for " + station
                                conn.rollback()
                            else:
                                conn.commit()
                            cur.close()
                        else:
                            pass
                print "Time taken to process " + filename + " : " + str(time.time() - start) + "secs"
                break

    def __init__(self):
        self.data = []

    if __name__ == '__main__': load_observation_data()
