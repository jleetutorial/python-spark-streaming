__author__ = 'hanhanw'

import sys
import re
import math

from pyspark import SparkConf, SparkContext

inputs1 = sys.argv[1]
inputs2 = sys.argv[2]
output = sys.argv[3]

conf = SparkConf().setAppName("correlate logs")
sc = SparkContext(conf=conf)

text = sc.textFile(inputs1) + sc.textFile(inputs2)


def parseline(line):
    linere = re.compile('^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$')
    match = re.search(linere, line)
    if match:
        m = re.match(linere, line)
        host = m.group(1)
        bys = float(m.group(4))
        return host, bys
    return None


def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a, b))


host_bytes = text.map(lambda line: parseline(line)).filter(lambda x: x is not None)\
    .map(lambda (host, bys): (host, (1, bys)))

xy_pairs = host_bytes.reduceByKey(lambda a, b: add_tuples(a, b)).coalesce(1)
xy_tuples = xy_pairs.map(lambda (k, (x, y)): ('same key', (x, y, pow(x, 2), pow(y, 2), x*y, 1)))
sum_xy_tuple = xy_tuples.reduceByKey(lambda a, b: add_tuples(a, b)).coalesce(1).map(lambda (k, t): t)


def calculate_r(t):
    n = t[5]
    sum_xy = t[4]
    sum_x = t[0]
    sum_y = t[1]
    sum_x2 = t[2]
    sum_y2 = t[3]
    r = (n*sum_xy - sum_x*sum_y)/(math.sqrt(n*sum_x2 - pow(sum_x, 2)) * math.sqrt(n*sum_y2 - pow(sum_y, 2)))
    return r, pow(r, 2)

rstr = 'r: '
r2str = 'r2: '

output_data = sum_xy_tuple.map(lambda t: calculate_r(t)).map(lambda (r, r2): (rstr, r, r2str, r2))

output_data.saveAsTextFile(output)

