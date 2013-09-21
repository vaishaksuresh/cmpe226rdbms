import sys
import csv
import os

class ExtractData:
	def loadFromFile():
		station,timestamp,mnet,latitude,longitude,elevation,temperature,sknt,direction,gust,pmsl,altitude,dewpoint,relhumidity,weather,p241 = ([] for i in range(16))
		for dirname, dirnames, filenames in os.walk('./out'):
			if '.git' in dirnames:
				# don't go into any .git directories.
				dirnames.remove('.git')
			for filename in filenames:
				print os.path.join(dirname, filename)
				with open(os.path.join(dirname, filename),'r') as csvfile:
					reader = csv.reader(csvfile)
					for line in reader:
						if len(line)>0 and len(line[0].split()) == 16:
							station.append(str(line[0]).split()[0])
							timestamp.append(str(line[0]).split()[1])
							mnet.append(str(line[0]).split()[2])
							latitude.append(str(line[0]).split()[3])
							longitude.append(str(line[0]).split()[4])
							elevation.append(str(line[0]).split()[5])
							temperature.append(str(line[0]).split()[6])
							sknt.append(str(line[0]).split()[7])
							direction.append(str(line[0]).split()[8])
							gust.append(str(line[0]).split()[9])
							pmsl.append(str(line[0]).split()[10])
							altitude.append(str(line[0]).split()[11])
							dewpoint.append(str(line[0]).split()[12])
							relhumidity.append(str(line[0]).split()[13])
							weather.append(str(line[0]).split()[14])
							p241.append(str(line[0]).split()[15])
						else:
							pass
			print "insert into weather values ('"+station[1]+"','"+timestamp[1]+"','"+temperature[1]+"','"+sknt[1]+"','"+direction[1]+"','"+gust[1]+"','"+pmsl[1]+"','"+altitude[1]+"','"+dewpoint[1]+"','"+relhumidity[1]+"','"+weather[1]+"','"+p241[1]+"')"

	if  __name__ =='__main__':loadFromFile()