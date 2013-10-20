__author__ = 'vaishaksuresh'
#!/usr/bin/python

import os
import csv
import MySQLdb
import time
import datetime


class Location(object):

    def __init__(self, arg):
        super(Location, self).__init__()
        self.arg = arg

    @staticmethod
    def glean_location_information():
        location_list = []
        temp_list = []
        start = time.time()
        current_list = Location.get_coordinates_as_list()
        print "Time Taken to get all locations : "+str(time.time()-start)
        start = time.time()
        for dir_name, dir_names, file_names in os.walk('../data/observation'):
            if '.git' in dir_names:
                dir_names.remove('.git')
            for filename in file_names:
                print "Processing: "+filename
                with open(os.path.join(dir_name, filename), 'r') as station_file:
                    reader = csv.reader(station_file)
                    for line in reader:
                        if len(line) > 0 and len(line[0].split()) == 16 and str(line[0]).split()[0] != 'STN':
                            t = float(str(line[0]).split()[3]), float(str(line[0]).split()[4]), \
                                float(str(line[0]).split()[5]), str(line[0]).split()[0]

                            if str(line[0]).split()[5] == '-0.00':
                                elev = '0.00'
                            else:
                                elev = str(line[0]).split()[5]
                            temp_coordinates = str(line[0]).split()[3]+str(line[0]).split()[4]+elev
                            if temp_coordinates not in current_list + temp_list:
                                location_list.append(t)
                                temp_list.append(temp_coordinates)
                print len(location_list)
            Location.insert_location_info_bulk(location_list)
            print "Time Taken to insert location information one by one: "
            print time.time()-start


    @staticmethod
    def get_coordinates_as_list():
        db = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = db.cursor()
        cur.execute("SELECT latitude, longitude, elevation FROM location")
        concatenated_list = []
        for row in cur.fetchall():
            concatenated_list.append(str('%.2f' % row[0])+str('%.2f' % row[1])+str('%.2f' % row[2]))
        return concatenated_list

    @staticmethod
    def insert_location_info(location_list):
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        for t in location_list:
            cur.execute("Insert into location (latitude, longitude, elevation, station_id) values (%s, %s, %s, %s)", t)
            con.commit()
        con.close()

    @staticmethod
    def insert_location_info_bulk(location_list):
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        for t in location_list:
            cur.execute("Insert into location (latitude, longitude, elevation, station_id) values (%s, %s, %s, %s)", t)
        con.commit()
        con.close()

    @staticmethod
    def truncate_location_table():
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        cur.execute("DELETE FROM location")
        cur.execute("ALTER TABLE location AUTO_INCREMENT = 1")
        con.commit()
        con.close()


class Observation(object):
    @staticmethod
    def truncate_observation_table():
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        cur.execute("DELETE FROM observation")
        con.commit()
        con.close()

    @staticmethod
    def glean_observation_information():
        #getting a list all station ids.
        station_list = Station.get_stations_as_list()
        #Getting a list of coordinates of all locations.
        location_list = Location.get_coordinates_as_list()
        for dir_name, dir_names, file_names in os.walk('../data/observation'):
            observation_list = []
            stations_to_insert = []
            locations_to_insert = []
            if '.git' in dir_names:
                # don't go into any .git directories.
                dir_names.remove('.git')
            for filename in file_names:
                print filename
                with open(os.path.join(dir_name, filename), 'r') as station_file:
                    reader = csv.reader(station_file)
                    for line in reader:
                        if len(line) > 0 and len(line[0].split()) == 16 and str(line[0]).split()[0] != 'STN':
                            row = line[0]

                            if row.split()[0] not in station_list:
                                stations_to_insert.append(tuple(row.split()[0]))
                            #making the elevation 0.00
                            if str(row).split()[5] == '-0.00':
                                elev = '0.00'
                            else:
                                elev = str(row).split()[5]
                            temp_coordinates = str(row).split()[3]+str(row).split()[4]+elev
                            #check if the database already has the location
                            if temp_coordinates in location_list:
                                pass
                                #get id here
                            else:
                                locations_tuple = str(row).split()[3], str(row).split()[4], elev, row.split()[0]
                                locations_to_insert.append(locations_tuple)

                            date_string = str(row).split()[1]
                            f = '%Y%m%d/%H%M'
                            t = str(row).split()[0], datetime.datetime.strptime(date_string, f), \
                                float(row.split()[6]), \
                                row.split()[7], row.split()[8], row.split()[9], \
                                row.split()[10], row.split()[11], row.split()[12], \
                                row.split()[13], row.split()[14], row.split()[15], 0
                            observation_list.append(t)

                    if len(stations_to_insert) > 0:
                        Station.insert_station_tuple_for_location(stations_to_insert)
                    if len(locations_to_insert) > 0:
                        Location.insert_location_info(locations_to_insert)

        #Observation.insert_observation_info(observation_list)
        Observation.insert_observation_info_bulk(observation_list)

    @staticmethod
    def insert_observation_info(observation_list):
        Observation.truncate_observation_table()
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        start = time.time()
        for t in observation_list:
            cur.execute("Insert IGNORE Into observation (station_id, observation_time, temperature, "
                        "sknt, wind_direction, gust, pmsl, altimeter, dew_point, relative_humidity, weather, "
                        "ptwentyfouri, location_id) Values (%s, %s, %s, %s, %s, %s, %s, %s, %s,"
                        " %s, %s, %s, %s)", t)
            con.commit()
        con.close()
        print "Time Taken to insert observation information one by one: "
        print time.time()-start

    @staticmethod
    def insert_observation_info_bulk(observation_list):
        Observation.truncate_observation_table()
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        start = time.time()
        for t in observation_list:
            cur.execute("Insert IGNORE Into observation (station_id, observation_time, temperature, "
                        "sknt, wind_direction, gust, pmsl, altimeter, dew_point, relative_humidity, weather, "
                        "ptwentyfouri, location_id) Values (%s, %s, %s, %s, %s, %s, %s, %s, %s,"
                        " %s, %s, %s, %s)", t)
        con.commit()
        con.close()
        print "Time Taken to bulk insert observation information: "
        print time.time()-start


class Station(object):

    @staticmethod
    def glean_station_information():
        station_list = []
        for dir_name, dir_names, file_names in os.walk('../data/station'):
            if '.git' in dir_names:
            # don't go into any .git directories.
                dir_names.remove('.git')
            for filename in file_names:
                print filename
                with open(os.path.join(dir_name, filename), 'r') as station_file:
                    reader = csv.reader(station_file)
                    for line in reader:
                        if line[0] != 'primary id':
                            t = line[0], line[2]
                            station_list.append(t)
            Station.insert_station_tuple(station_list)
            Station.insert_station_tuple_bulk(station_list)

    @staticmethod
    def insert_station_tuple(station_list):
        Station.truncate_station_table()
        start = time.time()
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        for t in station_list:
            cur.execute("INSERT IGNORE INTO station (station_id, station_name) values (%s, %s) ", t)
            con.commit()
        con.close()
        print "Time Taken to insert station information one by one: "
        print time.time()-start

    @staticmethod
    def insert_station_tuple_for_location(station_list):
        Station.truncate_station_table()
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        for t in station_list:
            cur.execute("INSERT IGNORE INTO station (station_id) values (%s) ", t)
        con.commit()
        con.close()
        print "All stations inserted"

    @staticmethod
    def insert_station_tuple_bulk(station_list):
        Station.truncate_station_table()
        start = time.time()
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        for t in station_list:
            cur.execute("INSERT IGNORE INTO station (station_id, station_name) values (%s, %s) ", t)
        con.commit()
        con.close()
        print "Time Taken to bulk insert station information: "
        print time.time()-start

    @staticmethod
    def truncate_station_table():
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        cur.execute("DELETE FROM station")
        con.commit()
        con.close()

    @staticmethod
    def get_stations_as_list():
        db = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = db.cursor()
        cur.execute("SELECT station_id FROM station")
        concatenated_list = []
        for row in cur.fetchall():
            concatenated_list.append(row[0])
        return concatenated_list