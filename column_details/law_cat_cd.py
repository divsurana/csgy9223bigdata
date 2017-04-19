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
    #lines = lines.take(500)
    #lines=sc.parallelize(lines)
    lines = lines.filter(lambda row: row != header)
    lines = lines.mapPartitions(lambda x: reader(x))
    counts = lines.map(lambda x: (x[11]))
    counts = counts.map(lambda x: (x, 1))
    counts = counts.map(lambda x: (x[0], pd_check_type(x[0]), pd_check_semantic(x[0]), pd_check_null(x[0])))

    def pd_check_null(pdstring):
        if (pdstring == ""):
            return 'NULL'
        elif (pdstring=='VIOLATION' or pdstring=='MISDEMEANOR' or pdstring=='FELONY'):
            return 'VALID'
        else:
            return 'INVALID'



    def pd_check_semantic(pdstring):
        counted=0
        if (pdstring == ''):
            return ''
        elif (pdstring=='COMPLETED' or pdstring=='ATTEMPTED'):
            return 'CRIME_LEVEL_CLASSIFICATION'
        else:
            return 'JUNK'


    def pd_check_type(pdstring):
        if isinstance(pdstring, str):
            return 'TEXT'
        elif isinstance(pdstring, int):
            return 'NUMBER'
        else:
            return ""


    counts = counts.map(lambda x: "%s\t%s\t%s\t%s" % (x[0], x[1], x[2], x[3]))

    counts.saveAsTextFile("crime_level_classifcation.out")

