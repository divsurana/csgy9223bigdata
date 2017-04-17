from __future__ import print_function
import sys
from operator import add
from csv import reader
import datetime
import re
from pyspark import SparkContext

if __name__ == "__main__":
	sc = SparkContext()
	lines = sc.textFile("crimes.csv")
	header = lines.first()
	# lines = lines.take(500)
	# lines=sc.parallelize(lines)
	lines = lines.filter(lambda row: row != header)
	lines = lines.mapPartitions(lambda x: reader(x))
	counts = lines.map(lambda x: (x[14]))
	counts = counts.map(
		lambda x: (x, add_pct_code_check_type(x), add_pct_code_check_semantic(x), add_pct_code_check_null(x)))
	
	
	def add_pct_code_check_null(add_pct_code_string):
		if add_pct_code_string == '':
			return 'NULL'
		else:
			try:
				int(add_pct_code_string)
				if int(add_pct_code_string) < 124 and int(add_pct_code_string) > 0:
					return 'VALID'
				else:
					return 'INVALID'
			except:
				return 'NULL'
	
	
	def add_pct_code_check_semantic(add_pct_code_string):
		if add_pct_code_string == '':
			return ''
		else:
			try:
				int(add_pct_code_string)
				return 'PRECINCT_ADDRESS_CODE'
			except:
				return 'JUNK'
	
	
	def add_pct_code_check_type(add_pct_code_string):
		if isinstance(add_pct_code_string, str):
			return 'TEXT'
		elif isinstance(add_pct_code_string, int):
			return 'NUMBER'
		else:
			return ""
	
	
	counts = counts.map(lambda x: "%s\t%s\t%s\t%s" % (x[0], x[1], x[2], x[3]))
	
	counts.saveAsTextFile("add_pct_code_details.out")
