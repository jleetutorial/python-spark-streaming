__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
import re
from sets import Set



conf = SparkConf().setAppName("entity resolution")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'
sqlCt = SQLContext(sc)

amazon_input = sys.argv[1]
google_input = sys.argv[2]
stopwords_input = sys.argv[3]
perfect_mapping_input = sys.argv[4]


def get_tokens(r, stopwords):
    tokens = re.split('\W+', r)
    tokens = [t.lower() for t in tokens if t not in stopwords and t!= u'']
    return tokens


def token_distribute(r):
    id = r[0]
    token_str = r[1]
    m = re.search('\[(.*?)\]', token_str)
    tokens = m.group(1).split(', ')
    token_id_map = [(t, id) for t in tokens]

    return token_id_map


def jaccard_similarity(r):
    m = re.search('\[(.*?)\],\[(.*?)\]', r)
    if m is None: return 0
    jk1 = Set(m.group(1).split(', '))
    jk2 = Set(m.group(2).split(', '))
    if len(jk1) == 0 or len(jk2) == 0: return 0

    common_tokens = [t for t in jk2 if t in jk1]
    combined_len = len(jk1) + len(jk2) - len(common_tokens)
    if combined_len == 0:
        return 0
    return float(len(common_tokens))/combined_len



class EntityResolution:
    def __init__(self, dataFile1, dataFile2, stopWordsFile):
        self.f = open(stopWordsFile, "r")
        self.stopWords = set(self.f.read().split("\n"))
        self.stopWordsBC = sc.broadcast(self.stopWords).value
        self.df1 = sqlCt.read.parquet(dataFile1).cache()
        self.df2 = sqlCt.read.parquet(dataFile2).cache()


    def preprocessDF(self, df, cols):
        stopwords = self.stopWordsBC
        transform_udf = udf(lambda r: get_tokens(r, stopwords))
        preprocessed_df = df.withColumn("joinKey", transform_udf(concat_ws(' ', df[cols[0]], df[cols[1]])))

        return preprocessed_df


    def filtering(self, df1, df2):
        flat_rdd1 = df1.select(df1.id, df1.joinKey).map(token_distribute).flatMap(lambda t: t)
        flat_rdd2 = df2.select(df2.id, df2.joinKey).map(token_distribute).flatMap(lambda t: t)

        flat_df1 = sqlCt.createDataFrame(flat_rdd1, ('token1', 'id1'))
        flat_df2 = sqlCt.createDataFrame(flat_rdd2, ('token2', 'id2'))

        cond = [flat_df1.token1 == flat_df2.token2]
        joined_df = flat_df2.join(flat_df1, cond).select(flat_df1.id1, flat_df2.id2).dropDuplicates()

        cond1 = [df1.id == joined_df.id1]
        new_df1 = joined_df.join(df1, cond1).select(joined_df.id1, df1.joinKey.alias('joinKey1'), joined_df.id2)

        cond2 = [df2.id == new_df1.id2]
        new_df2 = new_df1.join(df2, cond2)\
            .select(new_df1.id1, new_df1.joinKey1, new_df1.id2, df2.joinKey.alias('joinKey2'))
        new_df2.show()

        return new_df2


    def verification(self, candDF, threshold):
        jaccard_udf = udf(lambda r: jaccard_similarity(r))
        jaccard_df = candDF.withColumn("jaccard", jaccard_udf(concat_ws(',', candDF.joinKey1, candDF.joinKey2)))
        return jaccard_df.where(jaccard_df.jaccard >= threshold)


    def evaluate(self, result, groundTruth):
        countR = len(result)
        lstT = [t for t in result if t in groundTruth]
        countT = float(len(lstT))
        precision = countT/countR

        countA = len(groundTruth)
        recall = countT/countA

        fmeasure = 2*precision*recall/(precision+recall)

        return (precision, recall, fmeasure)


    def jaccardJoin(self, cols1, cols2, threshold):
        newDF1 = self.preprocessDF(self.df1, cols1)
        newDF1.show(n=1,truncate=False)
        newDF2 = self.preprocessDF(self.df2, cols2)
        newDF2.show(n=1,truncate=False)
        print "Before filtering: %d pairs in total" %(self.df1.count()*self.df2.count())

        candDF = self.filtering(newDF1, newDF2)
        print "After Filtering: %d pairs left" %(candDF.count())

        resultDF = self.verification(candDF, threshold)
        resultDF.show(truncate=False)
        print "After Verification: %d similar pairs" %(resultDF.count())

        return resultDF


    def __del__(self):
        self.f.close()


if __name__ == "__main__":
    er = EntityResolution(amazon_input, google_input, stopwords_input)
    amazonCols = ["title", "manufacturer"]
    googleCols = ["name", "manufacturer"]
    resultDF = er.jaccardJoin(amazonCols, googleCols, 0.5)
    resultDF.show(truncate=False)

    result = resultDF.map(lambda row: (row.id1, row.id2)).collect()
    groundTruth = sqlCt.read.parquet(perfect_mapping_input) \
                          .map(lambda row: (row.idAmazon, row.idGoogle)).collect()
    print "(precision, recall, fmeasure) = ", er.evaluate(result, groundTruth)
