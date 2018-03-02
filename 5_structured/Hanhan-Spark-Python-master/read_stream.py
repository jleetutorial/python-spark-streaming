__author__ = 'hanhanw'

import sys
import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


port = int(sys.argv[1])
output = sys.argv[2]


def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a, b))


def f(rdd):
    ls = rdd.map(lambda l: l.split(' ')).map(lambda (x, y): (float(x), float(y)))
    (xy_sum, x_sum, y_sum, x2_sum, n) = ls.map(lambda (x, y): (x*y, x, y, x*x, 1)).reduce(add_tuples)
    xy_avg = xy_sum/n
    x_avg = x_sum/n
    y_avg = y_sum/n
    x2_avg = x2_sum/n
    xavg_yavy = x_avg*y_avg
    xavg2 = x_avg*x_avg

    beta = (xy_avg - xavg_yavy)/(x2_avg - xavg2)
    alpha = y_avg - beta*x_avg

    rdd = sc.parallelize([(alpha, beta)], numSlices=1)
    rdd.saveAsTextFile(output + '/' + datetime.datetime.now().isoformat().replace(':', '-'))


sc = SparkContext()
ssc = StreamingContext(sc, 5)
lines = ssc.socketTextStream("cmpt732.csil.sfu.ca", port)
lines.foreachRDD(f)

ssc.start()
ssc.awaitTermination(timeout=300)
