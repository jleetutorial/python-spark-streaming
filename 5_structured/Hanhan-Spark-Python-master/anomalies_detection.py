__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import udf
import operator

conf = SparkConf().setAppName("anomalies detection")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'
sqlCt = SQLContext(sc)

inputs = sys.argv[1]

def to_onehot(lst, indices, unique_values, c):
    zs = [0.0]*c
    rest_lst = [float(lst[k]) for k in range(len(lst)) if k not in indices]
    for pos in indices:
        idx = unique_values.index(Row(lst[pos]))
        zs[idx] = 1.0
    zs.extend(rest_lst)
    return zs

class AnomalyDetection():

    def readData(self, filename):
        self.rawDF = sqlCt.read.parquet(filename).cache()


    def cat2Num(self, df, indices):
        unique_values = []
        for i in indices:
            d = udf(lambda r: r[i], StringType())
            dt = df.select(d(df.rawFeatures)).distinct().collect()
            unique_values.extend(dt)

        unique_count = len(unique_values)
        convertUDF = udf(lambda r: to_onehot(r, indices, unique_values, unique_count), ArrayType(DoubleType()))
        newdf = df.withColumn("features", convertUDF(df.rawFeatures))

        return newdf


    def addScore(self, df):
        cluster_dict = {}
        clusters_list = df.select("prediction").collect()
        for c in clusters_list:
            cluster_dict[c] = cluster_dict.setdefault(c,0.0)+1.0
        sorted_clusters = sorted(cluster_dict.items(), key=operator.itemgetter(1))  # sort by value
        n_max = sorted_clusters[-1][1]
        n_min = sorted_clusters[0][1]
        score_udf = udf(lambda p: float(n_max - cluster_dict.get(Row(p)))/(n_max - n_min), DoubleType())
        score_df = df.withColumn("score", score_udf(df.prediction))
        return score_df


    def detect(self, k, t):
        #Encoding categorical features using one-hot.
        df1 = self.cat2Num(self.rawDF, [0, 1]).cache()
        df1.show(n=2, truncate=False)

        #Clustering points using KMeans
        features = df1.select("features").rdd.map(lambda row: row[0]).cache()
        model = KMeans.train(features, k, maxIterations=40, runs=10, initializationMode="random", seed=20)

        #Adding the prediction column to df1
        modelBC = sc.broadcast(model)
        predictUDF = udf(lambda x: modelBC.value.predict(x), StringType())
        df2 = df1.withColumn("prediction", predictUDF(df1.features)).cache()
        df2.show(n=3, truncate=False)

        #Adding the score column to df2; The higher the score, the more likely it is an anomaly
        df3 = self.addScore(df2).cache()
        df3.show(n=3, truncate=False)

        return df3.where(df3.score > t)

def main():
    ad = AnomalyDetection()
    ad.readData(inputs)
    anomalies = ad.detect(8, 0.97)
    print anomalies.count()
    anomalies.show()

if __name__ == "__main__":
    main()
