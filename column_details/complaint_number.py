from __future__ import print_function

import sys
from operator import add
from csv import reader
import datetime
import re
from spark import SparkContext


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile("crimes")
    header = lines.first()
    lines = lines.filter(lambda row: row != header)
    lines = lines.mapPartitions(lambda x: reader(x))
    counts = lines.map(lambda x: (x[0]))
    #counts = counts.map(lambda x: (x, 1))
    counts = counts.map(lambda x: (x[0], complaint_check_type(x[0]), complaint_check_semantic(x[0]), complaint_check_null(x[0])))

    def complaint_check_null(complaint_string):
        if (complaint_string == ""):
            return 'NULL'
        else:
            try:
                int(s)
                if (int(complaint_string)<1000000000 and int(complaint_string)>99999999):
                    return 'VALID'
                else:
                    return 'INVALID'
            except:
                return 'NULL'



    def complaint_check_semantic(complaint_string):
        if (complaint_string == ''):
            return ''
        else:
            try:
                int(complaint_string)
                return 'complaint_NUMBER'
            except:
                return 'junk'



    def complaint_check_type(complaint_string):
        if isinstance(complaint_string, str):
            return 'TEXT'
        elif isinstance(complaint_string, int):
            return 'NUMBER'
        else:
            return ""


    counts = counts.map(lambda x: "%s\t%s\t%s\t%s" % (x[0], x[1], x[2], x[3]))

    counts.saveAsTextFile("complain_num_details.out")
