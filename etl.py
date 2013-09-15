import sys
import csv
from tokenize import generate_tokens
import re

class ExtractData:
	def loadFromFile():
		station=[]
		timestamp=[]
		with open('textfile','r') as csvfile:
			reader = csv.reader(csvfile)
			for line in reader:
				station.append(str(line[0]).split()[0])
				timestamp.append(str(line[0]).split()[1])
			print len(station)
			print len(timestamp)
				
	if  __name__ =='__main__':loadFromFile()