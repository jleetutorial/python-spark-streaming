__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.fpm import FPGrowth

inputs1 = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName("itemsets")
sc = SparkContext(conf=conf)

text = sc.textFile(inputs1)


def split_items(ts):
    items_list = []
    for transaction in ts:
        items = transaction.split()
        int_items = [int(i) for i in items]
        items_list.append(int_items)
    return items_list


transactions = text.map(lambda line: line).collect()
transaction_list = split_items(transactions)
rdd = sc.parallelize(transaction_list, 6)
model = FPGrowth.train(rdd, minSupport=0.002, numPartitions=10)
frequent_sets = model.freqItemsets().collect()

frequent_tuples = sc.parallelize(frequent_sets).map(lambda (items, freq): (sorted(items), freq)).coalesce(1).collect()
frequent_tuples.sort(key = lambda r: r[0])
frequent_tuples.sort(key = lambda r: r[1], reverse = True)

top10k = sc.parallelize(frequent_tuples).take(10000)
output_data = sc.parallelize(top10k).coalesce(1)

output_data.saveAsTextFile(output)


