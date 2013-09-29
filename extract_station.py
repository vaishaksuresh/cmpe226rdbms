import sys
import csv
import os


class ExtractData:
    def loadfromfile():
        for dirname, dirnames, filenames in os.walk('./tbl'):
            if '.git' in dirnames:
                # don't go into any .git directories.
                dirnames.remove('.git')
            for filename in filenames:
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
                            q = "insert into station_station (station_id,station_name,latitude, longitude,elevation,mesonet_id) values " \
                                "('"+primary_id+"','"+station_name+"','"+latitude+"','"+longitude+"','"+elevation+"','"+mesonet_id+"')"
                            with open ('station_inserts.sql', 'a') as f:
                                f.write(q+'\n')
                        else:
                            pass

    def __init__(self):
        self.data = []
    if __name__ == '__main__':
        loadfromfile()
