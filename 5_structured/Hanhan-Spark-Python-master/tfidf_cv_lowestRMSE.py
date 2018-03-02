__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.feature import Normalizer
import re
import operator
import math

conf = SparkConf().setAppName("733 A2 Q2 with cross validation")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

training_inputs = sys.argv[1]
testing_inputs = sys.argv[2]
output = sys.argv[3]

def parse_point(line):
    ptn1 = "\(([\d\.]*),\sSparseVector\((.*?)\)\)"
    ptn2 = "(\d+),\s+\{(.*?)\}"
    m = re.search(ptn1, line)
    if m:
        label = float(m.group(1))
        features_str = m.group(2)
        mx = re.search(ptn2, features_str)
        num = float(mx.group(1))
        fs = mx.group(2)
        idx_set = []
        tfidf_scores = []
        if fs != '':
            fs_split = fs.split(', ')
            for f in fs_split:
                idx_set.append(f.split(': ')[0])
                tfidf_scores.append(f.split(': ')[1])
        sp = SparseVector(num, idx_set, tfidf_scores)
        LP = LabeledPoint(label, sp)
        return LP
    return None


# Find the best step_size through cross validation, using RMSE as the error measurement
def get_best_stepsize(step_sizes, training_lp, iterations, cv_trails):
    best_stepsize = 0
    lowest_RMSE = float("inf")
    num_folds = 4
    fold_set = [1]*num_folds
    cv_data = training_lp.randomSplit(fold_set) # 4 folds
    for step_size in step_sizes:
        total_RMSE = 0.0
        for i in range(num_folds):
            cv_testing = cv_data[i]
            cv_training = training_lp.subtract(cv_testing)
            model = LinearRegressionWithSGD.train(cv_training, iterations=iterations, step=step_size)
            values_and_preds = cv_testing.map(lambda p: (p.label, model.predict(p.features)))
            MSE = values_and_preds.map(lambda (v, p): (v-p)**2).reduce(operator.add)
            RMSE = math.sqrt(MSE)
            total_RMSE += RMSE
        avg_RMSE = total_RMSE/cv_trails
        if avg_RMSE < lowest_RMSE:
            lowest_RMSE = avg_RMSE
            best_stepsize = step_size

    return best_stepsize


# Get the lowest RMSE after getting the best step size through cross validation
def get_best_result(best_step_size, training_lp, testing_lp, iterations):
    model = LinearRegressionWithSGD.train(training_lp, iterations=iterations, step=best_step_size)
    values_and_preds = testing_lp.map(lambda p: (p.label, model.predict(p.features)))
    MSE = values_and_preds.map(lambda (v, p): (v-p)**2).reduce(operator.add)
    RMSE = math.sqrt(MSE)

    result_str = 'best step size got by cross validation cv: ' + str(best_step_size) + ', lowest RMSE: ' + str(RMSE)
    return result_str


def main():
    training_data = sc.textFile(training_inputs)
    testing_data = sc.textFile(testing_inputs)

    training_LP = training_data.map(parse_point).filter(lambda result: result is not None)
    testing_LP = testing_data.map(parse_point).filter(lambda result: result is not None)

    t1 = range(1, 10)
    s2 = [t/10.0 for t in t1]
    step_sizes = s2
    iterations = 100
    cv_trails = 30

    best_step_size = get_best_stepsize(step_sizes, training_LP, iterations, cv_trails)
    best_result = get_best_result(best_step_size, training_LP, testing_LP, iterations)

    outdata = sc.parallelize([best_result])
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    main()
