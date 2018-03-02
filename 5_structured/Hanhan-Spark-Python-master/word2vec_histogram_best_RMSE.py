import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.feature import Normalizer
import re
import json
import operator
import math
import nltk
import string
import time
import numpy as np
from sets import Set


conf = SparkConf().setAppName("733 A2 Q7")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

review_inputs = sys.argv[1]
clusters_inputs = sys.argv[2]
output = sys.argv[3]


def parse_line(input_line):
    ptn = "\(u'(.*?)',\s(\d+)\)"
    m = re.search(ptn, input_line)
    word = m.group(1)
    cluster_idx = m.group(2)
    return word, int(cluster_idx)


def clean_review(review_line):
    pyline = json.loads(review_line)
    review_text = str(pyline['reviewText'])
    replace_punctuation = string.maketrans(string.punctuation, ' '*len(string.punctuation))
    review_text = review_text.translate(replace_punctuation).split()
    review_words = [w.lower() for w in review_text]

    pyline['reviewText'] = review_words
    pyline['overall'] = float(pyline['overall'])
    pyline['reviewTime'] = time.strptime(pyline['reviewTime'], '%m %d, %Y')
    return pyline['overall'], pyline['reviewText'], pyline['reviewTime']


def myf(x):
    return x


def get_review_histogram(t):
    ridx = t[0]
    cidx_lst = t[1]
    unique_cidxs = Set(cidx_lst)
    unique_cidxs = list(unique_cidxs)
    unique_cidxs.sort()
    total_clusters = 2000
    cluster_records = np.zeros(total_clusters)
    for cidx in cidx_lst:
        cluster_records[cidx] += 1
    sum_records = np.sum(cluster_records)
    l1_cluster_records = [x/sum_records for x in cluster_records]
    sparse_records = [x for x in l1_cluster_records if x > 0]
    sp_size = total_clusters
    sp = SparseVector(sp_size, unique_cidxs, sparse_records)
    return ridx, sp


def get_lp(t):
    rating = t[0]
    sp = t[1]
    return LabeledPoint(rating, sp)


def get_best_stepsize(step_sizes, training_lp, testing_lp, iterations):
    best_stepsize = 0
    lowest_RMSE = float("inf")
    for step_size in step_sizes:
        model = LinearRegressionWithSGD.train(training_lp, iterations=iterations, step=step_size)
        values_and_preds = testing_lp.map(lambda p: (p.label, model.predict(p.features)))
        MSE = values_and_preds.map(lambda (v, p): (v-p)**2).reduce(operator.add)
        RMSE = math.sqrt(MSE)
        if RMSE < lowest_RMSE:
            lowest_RMSE = RMSE
            best_stepsize = step_size

    result_str = 'best step size: ' + str(best_stepsize) + ', lowest RMSE: ' + str(lowest_RMSE)
    return result_str



def main():
    reviews_txt = sc.textFile(review_inputs)
    clusters_txt = sc.textFile(clusters_inputs)
    cluster_lookup_rdd = clusters_txt.map(parse_line)

    nltk_data_path = "[use your own nltk_data location]"  # maybe changed to the sfu server path
    nltk.data.path.append(nltk_data_path)
    cleaned_reviews = reviews_txt.map(clean_review).cache()
    training_reviews = cleaned_reviews.filter(lambda (rating, review_text, review_date): review_date.tm_year < 2014)
    testing_reviews = cleaned_reviews.filter(lambda (rating, review_text, review_date): review_date.tm_year == 2014)
    training_data = training_reviews.map(lambda (rating, review_text, review_date): (rating, review_text)).zipWithIndex().cache()
    testing_data = testing_reviews.map(lambda (rating, review_text, review_date): (rating, review_text)).zipWithIndex().cache()

    training_rating = training_data.map(lambda ((rating, review_text), review_index): (review_index, rating))
    training_review_text = training_data.map(lambda ((rating, review_text), review_index): (review_index, review_text))
    training_review_text_flat = training_review_text.flatMapValues(myf)
    training_review_text_flat = training_review_text_flat.map(lambda (review_index, review_word): (review_word, review_index))

    testing_rating = testing_data.map(lambda ((rating, review_text), review_index): (review_index, rating))
    testing_review_text = testing_data.map(lambda ((rating, review_text), review_index): (review_index, review_text))
    testing_review_text_flat = testing_review_text.flatMapValues(myf)
    testing_review_text_flat = testing_review_text_flat.map(lambda (review_index, review_word): (review_word, review_index))


    training_word_ridx_cidx = training_review_text_flat.join(cluster_lookup_rdd)
    training_ridx_cidx = training_word_ridx_cidx.map(lambda (w, (ridx, cidx)): (ridx, cidx)).groupByKey().mapValues(list).coalesce(1)
    training_histogram = training_ridx_cidx.map(get_review_histogram)
    training_rating_sp = training_rating.join(training_histogram).map(lambda x: (x[1][0], x[1][1]))
    training_lps = training_rating_sp.map(get_lp)

    testing_word_ridx_cidx = testing_review_text_flat.join(cluster_lookup_rdd)
    testing_ridx_cidx = testing_word_ridx_cidx.map(lambda (w, (ridx, cidx)): (ridx, cidx)).groupByKey().mapValues(list).coalesce(1)
    testing_histogram = testing_ridx_cidx.map(get_review_histogram)
    testing_rating_sp = testing_rating.join(testing_histogram).map(lambda x: (x[1][0], x[1][1]))
    testing_lps = testing_rating_sp.map(get_lp)

    t1 = range(1, 10)
    t2 = range(1, 5)
    s1 = [t/100.0 for t in t1]
    s2 = [t/10.0 for t in t2]
    step_sizes = s1
    step_sizes.extend(s2)
    iterations = 100

    result_str = 'before normalization: ' + get_best_stepsize(step_sizes, training_lps, testing_lps, iterations)
    outdata = sc.parallelize([result_str])
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    main()
