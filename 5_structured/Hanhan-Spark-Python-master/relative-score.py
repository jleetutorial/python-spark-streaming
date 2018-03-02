__author__ = 'hanhanw'

import sys
import json
from pyspark import SparkConf, SparkContext

inputs1 = sys.argv[1]
inputs2 = sys.argv[2]
output = sys.argv[3]

conf = SparkConf().setAppName("relative score")
sc = SparkContext(conf=conf)

text = sc.textFile(inputs1) + sc.textFile(inputs2)


def parseline(line):
    pyline = json.loads(line)
    return pyline


def add_tuples(t1, t2):
    return tuple(sum(p) for p in zip(t1, t2))

comments = text.map(lambda line: parseline(line)).cache()
commentbysub = comments.map(lambda c: (c['subreddit'], c))

c_score = comments.map(lambda c: (c['subreddit'], (c['score'], 1)))
c_scoresum = c_score.reduceByKey(lambda t1, t2: add_tuples(t1, t2))
c_avgscore = c_scoresum.map(lambda (k, (score_sum, count)): (k, float(score_sum/count))).filter(lambda (k, v): v>0).coalesce(1)

reddits = commentbysub.join(c_avgscore)
output_data = reddits.map(lambda (k, (c, avg)): (k, c['score']/avg, c['author'])).sortBy(lambda t: t[1], False)

output_data.saveAsTextFile(output)
