__author__ = 'hanhanw'

import sys
import re
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import levenshtein
from pyspark.mllib.recommendation import ALS, Rating

conf = SparkConf().setAppName("movies recommendations")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
assert sc.version >= '1.5.1'

movies_input = sys.argv[1]
user_ratings_input = sys.argv[2]
ratings_input = sys.argv[3]
output = sys.argv[4]

movies_original = sc.textFile(movies_input)
user_ratings_original = sc.textFile(user_ratings_input)
ratings_original = sc.textFile(ratings_input)

def parse_movies(movie):
    lst = movie.split('::')
    return lst[0], lst[1]  # movie_id, movie_name

def parse_ratings(rating):
    lst = rating.split('::')
    return lst[0], lst[1], lst[2]  # user_id, movie_id, rating

def parse_user_ratings(user_rating):
    ptn = re.compile('^(\\d+)\\s([\\w\\s:\(\)]+)')
    m = re.match(ptn, user_rating)
    if m:
        rating = m.group(1)
        user_movie_name = m.group(2)
        return user_movie_name, rating
    return None

def register_user_movies(user_ratings_list, movies):
    result = sc.parallelize([])
    for user_rating in user_ratings_list:
        df = movies.map(lambda (movie_id, movie_name): (movie_id, movie_name, user_rating[0], user_rating[1])).toDF()
        df.registerTempTable('MovieTable')
        dist_table = sqlContext.sql("""
        SELECT _1 as movie_id, _2 as movie_name, _3 as user_movie_name, _4 as user_rating, levenshtein(_2, _3) as distance FROM MovieTable
        """)
        dist_table.registerTempTable('DistanceTable')
        closest_df = sqlContext.sql("""
        SELECT * FROM DistanceTable
        ORDER BY distance
        LIMIT 1
        """)
        renamed_user_rating = closest_df.rdd.map(lambda (m_id, m, um, r, d): (m_id, m, r))
        result = result.union(renamed_user_rating)
    return result


def main():
    movies = movies_original.map(parse_movies).cache()
    ratings = ratings_original.map(parse_ratings).cache()
    user_ratings = user_ratings_original.map(parse_user_ratings).filter(lambda x: x is not None).cache().coalesce(1)
    user_ratings_list = user_ratings.collect()

    standard_user_ratings = register_user_movies(user_ratings_list, movies).cache()
    user_r = standard_user_ratings.map(lambda (m_id, m, rating): Rating(40000, int(m_id), float(rating)))
    u_movies = standard_user_ratings.map(lambda (m_id, m, rating): (m_id, m)).collect()
    r = ratings.map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))
    r = r.union(user_r)
    rank = 7
    numIterations = 10
    model = ALS.train(r, rank, numIterations)
    testdata = sc.parallelize(list(set(movies.collect())-set(u_movies))).map(lambda x: (40000, int(x[0])))

    predictions = model.predictAll(testdata).map(lambda x: (x[1], x[2]))  # movie_id, score

    results = movies.map(lambda (movie_id, m): (int(movie_id), m)).join(predictions)
    outdata = results.map(lambda x: (x[0], x[1][0], x[1][1])).sortBy(lambda l: l[2], ascending=False)
    outdata.saveAsTextFile(output)


if __name__ == "__main__":
    main()
