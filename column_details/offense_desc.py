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
    #lines = lines.take(500)
    #lines=sc.parallelize(lines)
    lines = lines.filter(lambda row: row != header)
    lines = lines.mapPartitions(lambda x: reader(x))
    counts = lines.map(lambda x: (x[7]))
    counts = counts.map(lambda x: (x, 1))
    counts = counts.map(lambda x: (x[0], offense_check_type(x[0]), offense_check_semantic(x[0]), offense_check_null(x[0])))

    def offense_check_null(offensestring):
        if (offensestring == ""):
            return 'NULL'
        else:
            try:
                int(offensestring)
                return 'INVALID'
            except:
                return 'VALID'



    def offense_check_semantic(offensestring):
        counted=0
        if (offensestring == ''):
            return ''
        else:
            try:
                int(offensestring)
                return 'JUNK'
            except:
                return 'OFFENSE_DESCRIPTION'


    def offense_check_type(offensestring):
        if isinstance(offensestring, str):
            return 'TEXT'
        elif isinstance(offensestring, int):
            return 'NUMBER'
        else:
            return ""


    counts = counts.map(lambda x: "%s\t%s\t%s\t%s" % (x[0], x[1], x[2], x[3]))

    counts.saveAsTextFile("offense_descriptions.out")
