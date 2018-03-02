__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, Rating
import operator
import math
import matplotlib.pyplot as plt


conf = SparkConf().setAppName("ALS")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'


training_inputs = sys.argv[1]
testing_inputs = sys.argv[2]


def get_tuple(line):
    elems = line.split('\t')
    return int(elems[0]), int(elems[1]), float(elems[2])

def to_Rating(t):
    return Rating(t[0], t[1], t[2])

def main():
    training_data = sc.textFile(training_inputs)
    testing_data = sc.textFile(testing_inputs)

    training_ratings = training_data.map(get_tuple).cache()
    testing_ratings = testing_data.map(get_tuple).cache()
    testing_all = testing_ratings.map(lambda (uid, mid, rating): (uid, mid)).cache()
    ratings = testing_ratings.map(to_Rating)


    ranks = [2, 4, 8, 16, 32, 64, 128, 256]
    reg_params = [0.1, 0.01]

    for i in range(len(reg_params)):
        RMSES = []
        for rank in ranks:
            model = ALS.train(training_ratings, rank=rank, lambda_=reg_params[i], seed=10)
            predictions = model.predictAll(testing_all).map(lambda r: ((r[0], r[1]), r[2]))
            ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
            MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
            RMSE = math.sqrt(MSE)
            RMSES.append(RMSE)
        plt.plot(range(len(ranks)), RMSES, label=str(reg_params[i]))

    plt.xticks(range(len(ranks)), ranks, size='small')
    plt.legend()
    plt.show()

if __name__ == '__main__':
    main()
