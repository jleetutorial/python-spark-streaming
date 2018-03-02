__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.mllib.feature import Word2Vec
from pyspark.mllib.clustering import KMeans
import nltk
import numpy as np
import string
import json

conf = SparkConf().setAppName("733 A2 Q5")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

inputs = sys.argv[1]
output = sys.argv[2]


def clean_review(review_line):
    pyline = json.loads(review_line)
    review_text = str(pyline['reviewText'])
    replace_punctuation = string.maketrans(string.punctuation, ' '*len(string.punctuation))
    review_text = review_text.translate(replace_punctuation).split()
    review_words = [w.lower() for w in review_text]

    return review_words

def generate_word2vec_model(doc):
    return Word2Vec().setVectorSize(10).setSeed(42).fit(doc)

def generate_kmeans_model(rdd, k):
    return KMeans.train(rdd, k, maxIterations=10, runs=30,
                                initializationMode="random", seed=50, initializationSteps=5, epsilon=1e-4)

def main():
    text = sc.textFile(inputs)

    nltk_data_path = "[cahnge to your own nltk_data location]"  # maybe changed to the sfu server path
    nltk.data.path.append(nltk_data_path)

    cleaned_review = text.map(clean_review)
    word2vec_model = generate_word2vec_model(cleaned_review)
    mv = word2vec_model.getVectors()  # this is a dictionary

    words_array = np.array(mv.values())
    k = 2000
    words_rdd = sc.parallelize(words_array)
    kmeans_model = generate_kmeans_model(words_rdd, k)

    unique_words = mv.keys()
    kmeans_predicts = []
    for unique_word in unique_words:
        vec = word2vec_model.transform(unique_word)
        kmeans_predict = kmeans_model.predict(vec)
        kmeans_predicts.append((unique_word, kmeans_predict))

    outdata = sc.parallelize(kmeans_predicts)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    main()
