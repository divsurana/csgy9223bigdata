from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime
import re

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile('crime.csv')
    header = lines.first()
    lines = lines.filter(lambda row: row != header)
    lines = lines.mapPartitions(lambda x: reader(x))
    counts = lines.map(lambda x: str(x[5]))
    counts = counts.map(lambda x: (x, 1))
    counts = counts.map(lambda x: (x[0], date_check_type(x[0]), date_check_semantic_type(x[0]), date_check_null(x[0])))


    def date_check_null(datestring):
        if (datestring == ""):
            return 'NULL'
        try:
            datetime.datetime.strptime(datestring, '%m/%d/%Y')
            m = re.search('(\d{1,2})/(\d{1,2})/(\d{4})', datestring)
            if m:
                found3 = m.group(3)
                found4 = int(found3)
                if found4 < 2012 or found4 > 2016:
                    return'INVALID'
            return 'VALID'
        except:
            return 'INVALID'


    def date_check_type(datestring):
        if (datestring == ""):
            return ""
        try:
            mat = re.match(r'(\d+/\d+/\d+)', datestring)

            if mat is not None:

                return 'DATETIME'
            else:
                return ""
        except:
            return ""


    def date_check_semantic_type(datestring):
        if (datestring == ''):
            return ''
        try:
            mat = re.match('^\d{1,2}\/\d{1,2}\/\d{4}$', datestring)
            if mat is not None:
                return 'COMPLAINT DATETIME'
            else:
                return " "
        except:
            return ""


    counts = counts.map(lambda x: "%s\t%s\t%s\t%s" % (x[0], x[1], x[2], x[3]))
    counts.saveAsTextFile("report_date_details.out")
