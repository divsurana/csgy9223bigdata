from __future__ import print_function
import re
import time
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()
	lines = sc.textFile('crime.csv')
	header = lines.first()
	lines = lines.filter(lambda row: row != header)
	lines = lines.mapPartitions(lambda x: reader(x))
	counts = lines.map(lambda x: (str(x[2])))
	counts = counts.map(lambda x: (x, 1))
	counts = counts.map(lambda x: (x[0], time_check_type(x[0]), time_check_semantic_type(x[0]), time_check_null(x[0])))
	
	
	def time_check_null(timestring):
		if timestring == "":
			return 'NULL'
		try:
			time.strptime(timestring, '%H:%M:%S')
			m = re.search('(\d{1,2}):(\d{1,2}):(\d{1,2})', timestring)
			if m:
				found1 = int(m.group(1))
				found2 = int(m.group(2))
				found3 = int(m.group(3))
				if found1 >= 24 or found1 < 0 or found2 > 60 or found2 < 0 or found3 > 60 or found3 < 0:
					return 'INVALID'
			return 'VALID'
		except:
			return 'INVALID'
	
	
	def time_check_type(timestring):
		if (timestring == ""):
			return ""
		try:
			mat = re.match('(\d{1,2}):(\d{1,2}):(\d{1,2})', timestring)
			
			if mat is not None:
				
				return 'TIME'
			else:
				return ""
		except:
			return ""
	
	
	def time_check_semantic_type(timestring):
		if (timestring == ''):
			return ''
		try:
			mat = re.match('(\d{1,2}):(\d{1,2}):(\d{1,2})', timestring)
			if mat is not None:
				return 'complaint TIME'
			else:
				return " "
		except:
			return ""
	
	
	counts = counts.map(lambda x: "%s\t%s\t%s\t%s" % (x[0], x[1], x[2], x[3]))
	counts.saveAsTextFile("complaint_time_details.out")
