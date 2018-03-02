__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
import nltk
import string
from nltk.corpus import stopwords
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
import json
import time

conf = SparkConf().setAppName("733 A2 Q1")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

inputs = sys.argv[1]
output_training = sys.argv[2]
output_testing = sys.argv[3]

def clean_review(review_line, stopwords):
    pyline = json.loads(review_line)
    review_text = str(pyline['reviewText'])
    replace_punctuation = string.maketrans(string.punctuation, ' '*len(string.punctuation))
    review_text = review_text.translate(replace_punctuation).split()
    review_words = [w.lower() for w in review_text if w not in stopwords]

    pyline['reviewText'] = review_words

    return pyline


def get_tfidf_features(txt):
    hashingTF = HashingTF()
    tf = hashingTF.transform(txt)
    tf.cache()
    idf = IDF().fit(tf)
    tfidf = idf.transform(tf)

    return tfidf


def get_output(in_data):
    text = sc.textFile(in_data)

    nltk_data_path = "[your nltk data]"  # chnage this t your nltk_data location
    nltk.data.path.append(nltk_data_path)
    stop_words = set(stopwords.words("english"))

    cleaned_review = text.map(lambda review_line: clean_review(review_line, stop_words))

    data_set = cleaned_review.map(lambda cleaned_line:
                                (cleaned_line['reviewText'], cleaned_line['overall'],
                                time.strptime(cleaned_line['reviewTime'], '%m %d, %Y'))).cache()

    training_data = data_set.filter(lambda (review_text, rating, review_date): review_date.tm_year < 2014).cache()
    training_ratings = training_data.map(lambda (review_text, rating, review_date): rating)
    training_reviews = training_data.map(lambda (review_text, rating, review_date): review_text)
    training_tfidf_features = get_tfidf_features(training_reviews)
    training_output = training_ratings.zip(training_tfidf_features).coalesce(1)


    testing_data = data_set.filter(lambda (review_text, rating, review_date): review_date.tm_year == 2014).cache()
    testing_ratings = testing_data.map(lambda (review_text, rating, review_date): rating)
    testing_reviews = testing_data.map(lambda (review_text, rating, review_date): review_text)
    testing_tfidf_features = get_tfidf_features(testing_reviews)
    testing_output = testing_ratings.zip(testing_tfidf_features).coalesce(1)

    return training_output, testing_output


def main():
    # return the rating and the SparseVector of the features
    training_output, testing_output = get_output(inputs)

    training_output.saveAsTextFile(output_training)
    testing_output.saveAsTextFile(output_testing)

if __name__ == '__main__':
    main()
