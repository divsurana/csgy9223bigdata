from __future__ import print_function

import sys
from operator import add
from csv import reader
import datetime
import re
from pyspark import SparkContext

if __name__ == "__main__":
	sc = SparkContext()
	lines = sc.textFile(sys.argv[1], 1)
	header = lines.first()
	# lines = lines.take(500)
	# lines=sc.parallelize(lines)
	lines = lines.filter(lambda row: row != header)
	lines = lines.mapPartitions(lambda x: reader(x))
	counts = lines.map(lambda x: (x[8]))
	counts = counts.map(lambda x: (x, pd_code_check_type(x), pd_code_check_semantic(x), pd_code_check_null(x)))
	
	
	def pd_code_check_null(pd_code_string):
		if pd_code_string == '':
			return 'NULL'
		else:
			try:
				int(pd_code_string)
				if 99 < int(pd_code_string) < 1000:
					return 'VALID'
				else:
					return 'INVALID'
			except:
				return 'NULL'
	
	
	def pd_code_check_semantic(pd_code_string):
		if pd_code_string == '':
			return ''
		else:
			try:
				int(pd_code_string)
				return 'PD_CODE'
			except:
				return 'JUNK'
	
	
	def pd_code_check_type(pd_code_string):
		if isinstance(pd_code_string, str):
			return 'TEXT'
		elif isinstance(pd_code_string, int):
			return 'NUMBER'
		else:
			return ""
	
	
	counts = counts.map(lambda x: "%s\t%s\t%s\t%s" % (x[0], x[1], x[2], x[3]))
	
	counts.saveAsTextFile("pd_code_details.out")
