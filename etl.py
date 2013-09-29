from __future__ import print_function
import sys
import csv
import os


class ExtractData:
    def loadFromFile():
        #station,timestamp,mnet,latitude,longitude,elevation,temperature,sknt,direction,gust,pmsl,altitude,dewpoint,relhumidity,weather,p241 = ([] for i in range(16))
        for dirname, dirnames, filenames in os.walk('./out'):
            if '.git' in dirnames:
                # don't go into any .git directories.
                dirnames.remove('.git')
            for filename in filenames:
                with open(os.path.join(dirname, filename),'r') as csvfile:
                    reader = csv.reader(csvfile)
                    for line in reader:
                        if len(line)>0 and len(line[0].split()) == 16 and str(line[0]).split()[0]!='STN':
                            station=str(line[0]).split()[0]
                            timestamp=str(line[0]).split()[1]
                            mnet = str(line[0]).split()[2]
                            latitude=str(line[0]).split()[3]
                            longitude=str(line[0]).split()[4]
                            elevation=str(line[0]).split()[5]
                            temperature=str(line[0]).split()[6]
                            sknt=str(line[0]).split()[7]
                            direction=str(line[0]).split()[8]
                            gust=str(line[0]).split()[9]
                            pmsl=str(line[0]).split()[10]
                            altitude=str(line[0]).split()[11]
                            dewpoint=str(line[0]).split()[12]
                            relhumidity=str(line[0]).split()[13]
                            weather=str(line[0]).split()[14]
                            p241=str(line[0]).split()[15]
                            #q = "insert into station_station values ('"+station+"','"+timestamp+"','"+temperature+"','"+sknt+"','"+direction+"','"+gust+"','"+pmsl+"','"+altitude+"','"+dewpoint+"','"+relhumidity+"','"+weather+"','"+p241+"')"
                            q = "insert into station_station (latitude, longitude, altitude,mesonet_id) values ('"+latitude+"','"+longitude+"','"+elevation+"','"+mnet+"','"+station+"')"
                            with open ('queries.sql', 'a') as f: f.write (q+'\n')
                        else:
                            pass

    if  __name__ =='__main__':loadFromFile()
