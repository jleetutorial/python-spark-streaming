__author__ = 'hanhanw'

import sys
import json
from pyspark import SparkConf, SparkContext

inputs1 = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName("relative score")
sc = SparkContext(conf=conf)

text = sc.textFile(inputs1)


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

avg_dict = dict(c_avgscore.collect())
bravg_dict = sc.broadcast(avg_dict)

def get_relative_score(bcast, comment_key, comment):
    avg_score = bcast.value.get(comment_key)
    score = comment['score']
    author = comment['author']
    relative_score = score/avg_score
    return comment_key, relative_score, author

output_data = commentbysub.map(lambda (k, c): get_relative_score(bravg_dict, k, c)).sortBy(lambda t: t[1], False)

output_data.saveAsTextFile(output)
