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
    counts = lines.map(lambda x: (x[9]))
    counts = counts.map(lambda x: (x, 1))
    counts = counts.map(lambda x: (x[0], pd_check_type(x[0]), pd_check_semantic(x[0]), pd_check_null(x[0])))

    def pd_check_null(pdstring):
        if (pdstring == ""):
            return 'NULL'
        else:
            try:
                int(pdstring)
                return 'INVALID'
            except:
                return 'VALID'



    def pd_check_semantic(pdstring):
        counted=0
        if (pdstring == ''):
            return ''
        else:
            try:
                int(pdstring)
                return 'JUNK'
            except:
                return 'PD_DESCRIPTION'


    def pd_check_type(pdstring):
        if isinstance(pdstring, str):
            return 'TEXT'
        elif isinstance(pdstring, int):
            return 'NUMBER'
        else:
            return ""


    counts = counts.map(lambda x: "%s\t%s\t%s\t%s" % (x[0], x[1], x[2], x[3]))

    counts.saveAsTextFile("pd_descriptions.out")
