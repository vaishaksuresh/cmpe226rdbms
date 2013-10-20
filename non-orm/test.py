#!/usr/bin/python
import MySQLdb
import time

db = MySQLdb.connect(host="localhost", user="root", passwd="", db="weather") 
cur = db.cursor() 

start = time.time()
# Use all the SQL you like
for i in range(0,100):
	cur.execute("select station.station_name from location, station where latitude between 37 and 40 and longitude < -76 and longitude > -120 and location.station_id = station.station_id")

db.close()
cur.close()
print "Average time taken for query"
print (time.time()-start)/100.0


