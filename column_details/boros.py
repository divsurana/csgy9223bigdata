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
    lines = lines.filter(lambda row: row != header)
    lines = lines.mapPartitions(lambda x: reader(x))
    counts = lines.map(lambda x: (x[13]))
    boros=['QUEENS','BROOKLYN','MANHATTAN','BRONX','STATEN ISLAND']
    counts = counts.map(lambda x: (x, 1))
    counts = counts.map(lambda x: (x[0], boro_check_type(x[0]), boro_check_semantic(x[0]), boro_check_null(x[0])))

    def boro_check_null(borostring):
        if borostring == "":
            return 'NULL'
        elif boro_check_semantic(borostring)=='Borough':
            return 'VALID'
        else:
            return 'INVALID'


    def boro_check_semantic(borostring):
        counted=0
        if borostring == '':
            return ''
        else:
            while counted<5:
                if borostring==boros[counted]:
                    counted=5
                    return 'Borough'
                else:
                    counted+=1
            return 'junk'



    def boro_check_type(borostring):
        if isinstance(borostring, str):
            return 'TEXT'
        elif isinstance(borostring, int):
            return 'NUMBER'
        else:
            return ""


    counts = counts.map(lambda x: "%s\t%s\t%s\t%s" % (x[0], x[1], x[2], x[3]))

    counts.saveAsTextFile("boro_details.out")

