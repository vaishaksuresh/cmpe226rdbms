#!/usr/bin/python
import sys
import os
import csv
import MySQLdb
import time


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

RunTests.read_station_files()
RunTests.read_station_files_bulk()

