from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext # From within pyspark or send to spark-submit
from pyspark.sql import Row,SQLContext
import sys
import requests

conf = SparkConf()

conf.setAppName("TwitterStreamApp")

sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 300) # 5 minute batch interval

ssc.checkpoint("checkpoint_TwitterApp")

dataStream = ssc.socketTextStream("localhost",9009) # Stream IP (localhost), and port (5555 in our case)

dataStream.pprint() # Print the incoming tweets to the console
﻿
ssc.start() # Start reading the stream
ssc.awaitTermination() # Wait for the process to terminate
