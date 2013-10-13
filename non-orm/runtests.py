#!/usr/bin/python
import sys
import os
import csv
import MySQLdb
import time
import datetime


class RunTests(object):

    def __init__(self, arg):
        super(RunTests, self).__init__()
        self.arg = arg

    @staticmethod
    def insert_station_tuple(t):
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        cur.execute("INSERT IGNORE INTO station (station_id, station_name) values (%s, %s) ", t)
        con.commit()
        con.close()

    @staticmethod
    def read_station_files():
        RunTests.truncate_station_table()
        for dir_name, dir_names, file_names in os.walk('../data/station'):
            start = time.time()
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
                            RunTests.insert_station_tuple(t)
            print "Time Taken to insert: "
            print time.time()-start

    @staticmethod
    def read_station_files_bulk():
        RunTests.truncate_station_table()
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        for dir_name, dir_names, file_names in os.walk('../data/station'):
            start = time.time()
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
                            cur.execute("INSERT IGNORE INTO station (station_id, station_name) values (%s, %s) ", t)
        con.commit()
        con.close()
        print "Time Taken to insert: "
        print time.time()-start

    @staticmethod
    def truncate_station_table():
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        cur.execute("DELETE FROM station")
        con.commit()
        con.close()

    @staticmethod
    def truncate_location_table():
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        cur.execute("DELETE FROM location")
        con.commit()
        con.close()

    @staticmethod
    def truncate_observation_table():
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        cur.execute("DELETE FROM observation")
        con.commit()
        con.close()

    @staticmethod
    def glean_location_information():
        for dir_name, dir_names, file_names in os.walk('../data/observation'):
            if '.git' in dir_names:
                # don't go into any .git directories.
                dir_names.remove('.git')
            for filename in file_names:
                print filename
                with open(os.path.join(dir_name, filename), 'r') as station_file:
                    reader = csv.reader(station_file)
                    for line in reader:
                        if len(line) > 0 and len(line[0].split()) == 16 and str(line[0]).split()[0] != 'STN':
                            t = str(line[0]).split()[3], str(line[0]).split()[4], str(line[0]).split()[5]
                            print t

    @staticmethod
    def glean_observation_information():
        for dir_name, dir_names, file_names in os.walk('../data/observation'):
            observation_list = []
            if '.git' in dir_names:
                # don't go into any .git directories.
                dir_names.remove('.git')
            for filename in file_names:
                print filename
                with open(os.path.join(dir_name, filename), 'r') as station_file:
                    reader = csv.reader(station_file)
                    for line in reader:
                        if len(line) > 0 and len(line[0].split()) == 16 and str(line[0]).split()[0] != 'STN':
                            date_string = str(line[0]).split()[1]
                            f = '%Y%m%d/%H%M'
                            t = str(line[0]).split()[0], datetime.datetime.strptime(date_string, f), \
                                float(line[0].split()[6]), \
                                line[0].split()[7], line[0].split()[8], line[0].split()[9], \
                                line[0].split()[10], line[0].split()[11], line[0].split()[12], \
                                line[0].split()[13], line[0].split()[14], line[0].split()[15], 0
                            observation_list.append(t)
        RunTests.insert_observation_info(observation_list)
        RunTests.insert_observation_info_bulk(observation_list)

    @staticmethod
    def insert_observation_info(observation_list):
        RunTests.truncate_observation_table()
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
        print "Time Taken to single insert: "
        print time.time()-start

    @staticmethod
    def insert_observation_info_bulk(observation_list):
        RunTests.truncate_observation_table()
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
        print "Time Taken to bulk insert: "
        print time.time()-start


#RunTests.read_station_files()
#RunTests.read_station_files_bulk()
#RunTests.glean_location_information()
RunTests.glean_observation_information()

