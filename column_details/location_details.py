from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import re

if __name__ == "__main__":
	sc = SparkContext()
	lines = sc.textFile(sys.argv[1], 1)
	header = lines.first()
	lines = lines.filter(lambda row: row != header)
	lines = lines.mapPartitions(lambda x: reader(x))
	latcounts = lines.map(lambda x: str(x[21]))
	longcounts = lines.map(lambda x: str(x[22]))
	
	latcounts = latcounts.map(lambda x: (x, 1))
	latcounts = latcounts.map(lambda x: (x[0], latlongcheck(x[0], True)))
	
	longcounts = longcounts.map(lambda x: (x, 1))
	longcounts = longcounts.map(
		lambda x: (x[0], latlongcheck(x[0], False)))
	
	
	def latlongcheck(coordinate, latlongboolean):
		check = []
		if coordinate == "":
			check = ['UNKNOWN', 'UNKNOWN', 'NULL']
		else:
			try:
				if re.match("^\d+\.\d+$", coordinate) is not None:
					if latlongboolean:
						if 40.49444444 <= float(coordinate) <= 45.01166667:
							check = ['FLOAT', 'Latitude', 'VALID']
						else:
							check = ['FLOAT', 'Latitude', 'INVALID']
					else:
						if -79.76500000 <= float(coordinate) <= -71.79027778:
							check = ['FLOAT', 'Longitude', 'VALID']
						else:
							check = ['FLOAT', 'Longitude', 'INVALID']
				else:
					check = ['UNKNOWN', 'UNKNOWN', 'INVALID']
			except:
				check = ['UNKNOWN', 'UNKNOWN', 'INVALID']
		return check
	
	
	latcounts = latcounts.map(lambda x: "%s\t%s\t%s\t%s" % (x[0], x[1][0], x[1][1], x[1][2]))
	longcounts = longcounts.map(lambda x: "%s\t%s\t%s\t%s" % (x[0], x[1][0], x[1][1], x[1][2]))
	
	latcounts.saveAsTextFile("report_latitude_details.out")
	longcounts.saveAsTextFile("report_longitude_details.out")
