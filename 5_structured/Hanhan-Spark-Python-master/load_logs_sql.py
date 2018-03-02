__author__ = 'hanhanw'

import sys
import re
import datetime

from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext


inputs1 = sys.argv[1]
inputs2 = sys.argv[2]
output = sys.argv[3]

conf = SparkConf().setAppName("load logs")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
assert sc.version >= '1.5.1'

text = sc.textFile(inputs1)+sc.textFile(inputs2)

def parseline(line):
    linere = re.compile('^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$')
    match = re.search(linere, line)
    if match:
        m = re.match(linere, line)
        host = m.group(1)
        dt = datetime.datetime.strptime(m.group(2), '%d/%b/%Y:%H:%M:%S')
        path = m.group(3)
        bys = float(m.group(4))
        dct = {"host": host, "datetime": dt, "path": path, "bys": bys}
        return dct
    return None

rdd = text.map(lambda line: parseline(line)).filter(lambda x: x is not None)
df = sqlContext.createDataFrame(rdd).coalesce(1)
df.write.format('parquet').save(output, mode='overwrite')






