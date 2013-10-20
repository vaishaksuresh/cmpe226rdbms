#!/usr/bin/python
import sys
import os
import csv
import MySQLdb
import time
import datetime
from data_access import Location,Station,Observation


class RunTests(object):

    def __init__(self, arg):
        super(RunTests, self).__init__()
        self.arg = arg

#Station.glean_station_information()
#Location.glean_location_information()
Observation.glean_observation_information()

