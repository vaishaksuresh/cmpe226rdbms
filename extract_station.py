import sys
import csv
import os
import psycopg2
import time


class ExtractData:
    def load_stations_to_db():
        for dirname, dirnames, filenames in os.walk('./tbl'):
            if '.git' in dirnames:
                # don't go into any .git directories.
                dirnames.remove('.git')
            for filename in filenames:
                start = time.time()
                with open(os.path.join(dirname, filename), 'r') as csvfile:
                    reader = csv.reader(csvfile)
                    for line in reader:

                        if line[0] != 'primary id':
                            primary_id = line[0]
                            secondary_id = line[1]
                            station_name = line[2]
                            latitude = line[5]
                            longitude = line[6]
                            elevation = line[7]
                            mesonet_id = line[8]
                            mesonet_name = line[9]
                            station_name = station_name.replace("'", r"\\'")

                            '''q = "insert into station_station (station_id,station_name,latitude,longitude,elevation,mesonet_id) values " \
                                "('"+primary_id+"','"+station_name+"','"+latitude+"','"+longitude+"','"+elevation+"','"+mesonet_id+"')"
                            with open ('station_inserts.sql', 'a') as f:
                                f.write(q+'\n')'''
                            try:
                                conn = psycopg2.connect("host='localhost' dbname=weather user=postgres")
                                cur = conn.cursor()
                                cur.execute("insert into station_station (station_id,station_name,latitude,longitude,elevation,mesonet_id) values (%s,%s,%s,%s,%s,%s)",(primary_id,station_name,latitude,longitude,elevation,mesonet_id))
                            except psycopg2.IntegrityError:
                                print "Integrity Exception for " + primary_id
                                conn.rollback()
                            except psycopg2.DataError:
                                print "Data Error! Something was null " + primary_id
                                conn.rollback()
                            else:
                                conn.commit()
                            cur.close()
                        else:
                            pass
                print "Time taken to process "+filename+" : "+str(time.time() - start) + "secs"
                break
    def __init__(self):
        self.data = []
    if __name__ == '__main__':
        load_stations_to_db()
