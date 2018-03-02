__author__ = 'hanhanw'

import sys
import operator
import re
import string
import unicodedata

from pyspark import SparkConf, SparkContext

inputs = sys.argv[1]
output = sys.argv[2]
wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

conf = SparkConf().setAppName("word count improved")
sc = SparkContext(conf=conf)

text = sc.textFile(inputs)


def parseline(line):
    nomalized_line = unicodedata.normalize('NFD', line)
    word_list = wordsep.split(nomalized_line)
    word_list = filter(None, word_list)
    return word_list

words = text.flatMap(lambda line: parseline(line)).map(lambda w: (w.lower(), 1))

wordcount = words.reduceByKey(operator.add).coalesce(1).cache()
wordcount_sorted = wordcount.sortBy(lambda (w, c): c, False)  # sort by frequency, high frequency first
# wordcount_sorted = wordcount.sortBy(lambda (w, c): w)  # sort by word
# wordcount_sorted = wordcount.sortBy(lambda (w, c): (c, w), False)  # sorted alphabetically within equal frequencies

outdata = wordcount_sorted.map(lambda (w, c): u"%s %i" % (w, c))
outdata.saveAsTextFile(output)
