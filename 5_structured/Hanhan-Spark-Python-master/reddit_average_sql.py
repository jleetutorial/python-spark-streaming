__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

conf = SparkConf().setAppName("reddit averages sql")
sc = SparkContext(conf=conf)

inputs1 = sys.argv[1]
# inputs2 = sys.argv[2]   # Uncomment this when there are 2 inputs dir
output = sys.argv[2]
sqlContext = SQLContext(sc)


def get_avg(comments):
    comments.registerTempTable('comments')
    avg_df = sqlContext.sql("""SELECT subreddit, AVG(score) FROM comments GROUP BY subreddit""").coalesce(1)
    return avg_df


def main():
    schema = StructType([
    StructField('subreddit', StringType(), False),
    StructField('score', IntegerType(), False),
    ])
    inputs = sqlContext.read.json(inputs1, schema=schema)

    # Uncomment this then shcema is not added
    # inputs = sqlContext.read.json(inputs1)

    # Uncomment these when there are 2 inputs dir
    # comments_input1 = sqlContext.read.json(inputs1, schema=schema)
    # comments_input2 = sqlContext.read.json(inputs2, schema=schema)
    # inputs = comments_input1.unionAll(comments_input2)

    df = get_avg(inputs)
    df.write.save(output, format='json', mode='overwrite')
if __name__ == "__main__":
    main()


