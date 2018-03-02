__author__ = 'hanhanw'

import sys
import random
import operator
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("euler")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

total_iterations = long(sys.argv[1])

def get_count(iter):
    count = 0
    for i in range(iter):
        sum = 0.0
        while (sum < 1):
            sum += random.random()
            count += 1
    return count


def main():
    partition_num = 100
    iteraion = total_iterations/partition_num

    lst = []
    for i in range(partition_num):
        lst.append(iteraion)

    iter_rdd = sc.parallelize(lst, partition_num)
    count_rdd = iter_rdd.map(get_count)
    s = count_rdd.reduce(operator.add)
    result = float(s)/total_iterations
    print s, total_iterations, result

if __name__ == "__main__":
    main()
