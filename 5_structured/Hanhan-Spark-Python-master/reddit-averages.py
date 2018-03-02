__author__ = 'hanhanw'

import sys
import json

from pyspark import SparkConf, SparkContext

inputs1 = sys.argv[1]
# inputs2 = sys.argv[2]
output = sys.argv[2]

conf = SparkConf().setAppName("reddit averages")
sc = SparkContext(conf=conf)

text = sc.textFile(inputs1)


def parseline(line):
    pyline = json.loads(line)
    key = pyline.get("subreddit")
    count = 1
    score = pyline.get("score")
    dct = {"key": key, "pair": (count, score)}
    return dct


def add_pairs((x1, y1), (x2, y2)):
    return x1+x2, y1+y2


dcts = text.map(lambda line: parseline(line))
lines = dcts.map(lambda dct: (dct.get("key"), dct.get("pair")))

reduced_lines = lines.reduceByKey(lambda (x1, y1), (x2, y2): add_pairs((x1, y1), (x2, y2))).coalesce(1).cache()
averaged_lines = reduced_lines.map(lambda (key, (count, score)): (key, float(score)/count))
json_lines = averaged_lines.map(lambda line: json.dumps(line))

json_lines.saveAsTextFile(output)
