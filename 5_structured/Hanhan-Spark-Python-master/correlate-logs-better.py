__author__ = 'hanhanwu'


import sys
import re
import operator
import math


from pyspark import SparkConf, SparkContext

inputs1 = sys.argv[1]
inputs2 = sys.argv[2]
output = sys.argv[3]

conf = SparkConf().setAppName("correlate logs better")
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

xy_pairs = host_bytes.reduceByKey(lambda a, b: add_tuples(a, b)).coalesce(1).map(lambda (k, (x, y)): (x, y)).cache()
pairs_count = len(xy_pairs.collect())
x_sum = xy_pairs.map(lambda (x, y): x).reduce(operator.add)
x_mean = float(x_sum/pairs_count)
y_sum = xy_pairs.map(lambda (x, y): y).reduce(operator.add)
y_mean = float(y_sum/pairs_count)

above = xy_pairs.map(lambda (x, y): ((x-x_mean) * (y-y_mean))).reduce(operator.add)
below_left = math.sqrt(xy_pairs.map(lambda (x, y): pow((x-x_mean), 2)).reduce(operator.add))
below_right = math.sqrt(xy_pairs.map(lambda (x, y): pow((y-y_mean), 2)).reduce(operator.add))

r = above/(below_left*below_right)
r2 = pow(r, 2)

test = sc.parallelize([r, r2]).coalesce(1)

test.saveAsTextFile(output)






















