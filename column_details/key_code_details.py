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
	counts = lines.map(lambda x: (x[6]))
	counts = counts.map(lambda x: (x, key_code_check_type(x), key_code_check_semantic(x), key_code_check_null(x)))
	
	
	def key_code_check_null(key_code_string):
		if key_code_string == '':
			return 'NULL'
		else:
			try:
				int(key_code_string)
				if 99 < int(key_code_string) < 1000:
					return 'VALID'
				else:
					return 'INVALID'
			except:
				return 'NULL'
	
	
	def key_code_check_semantic(key_code_string):
		if key_code_string == '':
			return ''
		else:
			try:
				int(key_code_string)
				return 'KEY_CODE'
			except:
				return 'JUNK'
	
	
	def key_code_check_type(key_code_string):
		if isinstance(key_code_string, str):
			return 'TEXT'
		elif isinstance(key_code_string, int):
			return 'NUMBER'
		else:
			return ""
	
	
	counts = counts.map(lambda x: "%s\t%s\t%s\t%s" % (x[0], x[1], x[2], x[3]))
	
	counts.saveAsTextFile("key_code_details.out")
