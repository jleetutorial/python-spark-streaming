__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.mllib.feature import Word2Vec
import nltk
import string
import json

conf = SparkConf().setAppName("733 A2 Q4")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
assert sc.version >= '1.5.1'

inputs = sys.argv[1]
model_output = sys.argv[2]
similar_words_output = sys.argv[3]


def clean_review(review_line):
    pyline = json.loads(review_line)
    review_text = str(pyline['reviewText'])
    replace_punctuation = string.maketrans(string.punctuation, ' '*len(string.punctuation))
    review_text = review_text.translate(replace_punctuation).split()
    review_words = [w.lower() for w in review_text]

    return review_words

def generate_word2vec_model(doc):
    return Word2Vec().setVectorSize(10).setSeed(42).fit(doc)

def get_similar_words(model, word, output_num):
    st = model.findSynonyms(word, output_num)
    outstr = 'similiar words for ' + word + ': '
    for i in range(len(st)):
        outstr += '(' + str(st[i][0]) + ', ' + str(st[i][1]) + '), '
    return outstr


def main():
    text = sc.textFile(inputs)

    nltk_data_path = "[change to your nltk_data location]"  # maybe changed to the sfu server path
    nltk.data.path.append(nltk_data_path)

    cleaned_review = text.map(clean_review)
    model = generate_word2vec_model(cleaned_review)
    mv = model.getVectors()

    # find similar words
    similar_words = []
    test_words = ['dog', 'happy']
    outnum = 2
    for w in test_words:
        outstr = get_similar_words(model, w, outnum)
        similar_words.append(outstr)

    # save the model
    results = []
    for k,v in mv.items():
        tmp_str = str(k) + ',['
        for f in v:
            tmp_str += str(f) + ', '
        tmp_str += ']'
        results.append(tmp_str)

    outmodel = sc.parallelize(results)
    out_similarwords = sc.parallelize(similar_words)
    outmodel.saveAsTextFile(model_output)
    out_similarwords.saveAsTextFile(similar_words_output)

if __name__ == '__main__':
    main()
