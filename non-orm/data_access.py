__author__ = 'vaishaksuresh'
#!/usr/bin/python
import sys
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
    def truncate_location_table():
        con = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather")
        cur = con.cursor()
        cur.execute("DELETE FROM location")
        con.commit()
        con.close()


class Observation(object):
    pass


class Station(object):
    pass