
# coding: utf-8

# # Integration with Kafka Demo

# Donwload this notebook as a .py file
# 
# For setting up the stream, First run the following scripts to create the zookeeper server:
# ```
# ~/kafka_2.11-0.11.0.0/bin/zookeeper-server-start.sh ~/kafka_2.11-0.11.0.0/config/zookeeper.properties
# ```
# Then setup the kafka server with this shell command:
# ```
# ~/kafka_2.11-0.11.0.0/bin/kafka-server-start.sh ~/kafka_2.11-0.11.0.0/config/server.properties
# ```
# to create the topic, use the following commands
# ```
# ~/kafka_2.11-0.11.0.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic pyspark-kafka-demo --replication-factor 1 --partitions 3
# ```
# 
# Donwload this notebook as a .py file and run the following:
# ```
# python3 pyspark-streaming/4_morestream/1_Integration_with_Kafka_Demo.py
# ```
# Finally, start the producer:
# ```
# ~/kafka_2.11-0.11.0.0/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic pyspark-kafka-demo
# ```
# 
# 

# ### Demo

# In[ ]:


import findspark
# TODO: your path will likely not have 'matthew' in it. Change it to reflect your path.
findspark.init('/home/matthew/spark-2.3.0-bin-hadoop2.7')

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 pyspark-shell'

import sys
import time
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


n_secs = 1
topic = "pyspark-kafka-demo"

conf = SparkConf().setAppName("KafkaStreamProcessor").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, n_secs)
    
kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {
                        'bootstrap.servers':'localhost:9092', 
                        'group.id':'video-group', 
                        'fetch.message.max.bytes':'15728640',
                        'auto.offset.reset':'largest'})
                        # Group ID is completely arbitrary

lines = kafkaStream.map(lambda x: x[1])
counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
counts.pprint()

ssc.start()
time.sleep(600) # Run stream for 10 minutes just in case no detection of producer
# ssc.awaitTermination()
ssc.stop(stopSparkContext=True,stopGraceFully=True)


# ## References
# 1. https://spark.apache.org/docs/latest/streaming-kafka-integration.html
# 2. https://spark.apache.org/docs/latest/streaming-programming-guide.html#performance-tuning
# 3. https://apache.googlesource.com/spark/+/master/examples/src/main/python/streaming/kafka_wordcount.py

#  
