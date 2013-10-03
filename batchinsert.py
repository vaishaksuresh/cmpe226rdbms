__author__ = 'vaishaksuresh'
import os
import time
import psycopg2


if __name__ == '__main__':
        start = time.time()
        conn = psycopg2.connect("host='localhost' dbname=weather user=postgres")
        cur = conn.cursor()
        with open('queries.sql', 'r') as f:
            cur.copy_from(f, "station_observation", sep=',', size=8192)
            conn.commit()
            cur.close()
        print "Time taken to process  : " + str(time.time() - start) + "secs"