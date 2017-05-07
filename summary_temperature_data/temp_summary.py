from __future__ import print_function
import re
import time
import datetime
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc=SparkContext()
    lines = sc.textFile('tempny.csv')
    header=lines.first()
    lines=lines.filter(lambda row : row !=header)
    lines= lines.mapPartitions( lambda x:reader(x))
    def proc1(a,b):
        z=(int(a)+int(b))/2
        return z
    
    def	proc(x):
        yy=x[0:4]
        mm=x[4:6]
        dd=x[6:8]
        z=mm+'/'+dd+'/'+yy
        return z
    
    lines=lines.map(lambda x :(proc(x[5]),proc1(x[9],x[10])))
    #retreiving date and temperature
    lines = lines.map(lambda x: "%s,%s" % (x[0], x[1]))
    lines.saveAsTextFile("temperature.csv")
