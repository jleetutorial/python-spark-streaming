__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import Rating
from pyspark.sql import SQLContext
import operator
import math


conf = SparkConf().setAppName("Slope One")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'
sqlContext = SQLContext(sc)


training_inputs = sys.argv[1]
testing_inputs = sys.argv[2]


def get_tuple(line):
    elems = line.split('\t')
    return int(elems[0]), int(elems[1]), float(elems[2])


def main():
    training_in = sc.textFile(training_inputs)
    testing_in = sc.textFile(testing_inputs)

    training_data = training_in.map(get_tuple)
    testing_data = testing_in.map(get_tuple).cache()

    training_df = sqlContext.createDataFrame(training_data, ['uid', 'mid', 'rating'])
    testing_df = sqlContext.createDataFrame(testing_data, ['uid', 'mid', 'rating'])

    training_df.registerTempTable("TrainingTable")
    testing_df.registerTempTable("TestingTable")

    joined_user_df = sqlContext.sql("""
    SELECT t1.uid, t1.mid as mid1, t2.mid as mid2, (t1.rating-t2.rating) as rating_diff FROM
    TrainingTable t1
    JOIN
    TrainingTable t2
    ON (t1.uid = t2.uid)
    """)

    joined_user_df.registerTempTable("JoinedUserTable")
    mpair_dev_c_df = sqlContext.sql("""
    SELECT mid1, mid2, sum(rating_diff)/count(rating_diff) as dev, count(rating_diff) as c FROM
    JoinedUserTable
    Group By mid1, mid2
    """)

    testing_training_df = sqlContext.sql("""
    SELECT t1.uid, t1.mid as midj, t2.mid as midi, t1.rating as rating_j, t2.rating as rating_i FROM
    TestingTable t1
    JOIN
    TrainingTable t2
    ON (t1.uid = t2.uid)
    """)

    cond = [testing_training_df.midj == mpair_dev_c_df.mid1, testing_training_df.midi == mpair_dev_c_df.mid2]
    df = testing_training_df.join(mpair_dev_c_df, cond)

    df.registerTempTable("AllTable")
    ps = sqlContext.sql("""
    SELECT uid, midj, sum((dev+rating_i)*c)/sum(c) as p, rating_j as true_rating FROM
    AllTable
    Group By uid, midj, rating_j
    """)

    ps.registerTempTable("PTable")
    rmse = sqlContext.sql("""
    SELECT sqrt(sum(power(true_rating-p, 2))/count(true_rating)) as RMSE FROM
    PTable
    """)
    rmse.show()

if __name__ == '__main__':
    main()
