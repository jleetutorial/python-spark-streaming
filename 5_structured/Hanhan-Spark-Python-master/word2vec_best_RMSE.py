__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
import nltk
import string
from pyspark.mllib.feature import HashingTF, IDF, Word2Vec
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
import json
import operator
import time
import math
import numpy as np

conf = SparkConf().setAppName("733 A2 Q6")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

inputs = sys.argv[1]
output= sys.argv[2]


def clean_reviewf(review_line):
    pyline = json.loads(review_line)
    review_text = str(pyline['reviewText'])
    replace_punctuation = string.maketrans(string.punctuation, ' '*len(string.punctuation))
    review_text = review_text.translate(replace_punctuation).split()
    review_words = [w.lower() for w in review_text]

    pyline['reviewText'] = review_words
    pyline['overall'] = float(pyline['overall'])
    pyline['reviewTime'] = time.strptime(pyline['reviewTime'], '%m %d, %Y')
    return pyline


def generate_word2vec_model(doc):
    return Word2Vec().setVectorSize(10).setSeed(42).fit(doc)


def myf(x):
    return x


def get_lp(t):
    rating = t[1][0]
    avg_features = t[1][1]
    return LabeledPoint(rating, avg_features)


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
    text = sc.textFile(inputs)

    nltk_data_path = "[change to your own nltk_data]"  # maybe changed to the sfu server path
    nltk.data.path.append(nltk_data_path)
    cleaned_review = text.map(clean_reviewf).cache()

    reviews_txt = cleaned_review.map(lambda review: review['reviewText'])
    reviews = cleaned_review.map(lambda review: (review['overall'], review['reviewText'], review['reviewTime'])).cache()
    training_reviews = reviews.filter(lambda (rating, review_text, review_date): review_date.tm_year < 2014)
    testing_reviews = reviews.filter(lambda (rating, review_text, review_date): review_date.tm_year == 2014)
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

    word2vec_model = generate_word2vec_model(reviews_txt)
    mv = word2vec_model.getVectors()
    # this step seems redundant but necessary
    mvdct = []
    for k,v in mv.items():
        vec = [f for f in v]
        mvdct.append((k,vec))
    dct_rdd = sc.parallelize(mvdct)

    training_feature_vecs = dct_rdd.join(training_review_text_flat)
    training_vecs = training_feature_vecs.map(lambda (w,(feature_vec, review_index)): (review_index, (feature_vec, 1)))
    training_reduce_vecs = training_vecs.reduceByKey(lambda v1,v2: (np.sum([v1[0],v2[0]], axis=0),v1[1]+v2[1]))
    training_avg_vecs = training_reduce_vecs.map(lambda (review_index, (feature_vec, ct)): (review_index, np.array(feature_vec)/float(ct)))
    training_rating_avgf = training_rating.join(training_avg_vecs)
    training_lps = training_rating_avgf.map(get_lp)

    testing_feature_vecs = dct_rdd.join(testing_review_text_flat)
    testing_vecs = testing_feature_vecs.map(lambda (w,(feature_vec, review_index)): (review_index, (feature_vec, 1)))
    testing_reduce_vecs = testing_vecs.reduceByKey(lambda v1,v2: (np.sum([v1[0],v2[0]], axis=0),v1[1]+v2[1]))
    testing_avg_vecs = testing_reduce_vecs.map(lambda (review_index, (feature_vec, ct)): (review_index, np.array(feature_vec)/float(ct)))
    testing_rating_avgf = testing_rating.join(testing_avg_vecs)
    testing_lps = testing_rating_avgf.map(get_lp)


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
