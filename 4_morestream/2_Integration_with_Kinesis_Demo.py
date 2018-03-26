
# coding: utf-8

# # Integration with Kinesis Demo

# More information here: https://docs.aws.amazon.com/streams/latest/dev/before-you-begin.html
# 
# and here: https://spark.apache.org/docs/latest/streaming-kinesis-integration.html

# Kinesis Data Generator: https://awslabs.github.io/amazon-kinesis-data-generator/
# 
# After going through the setup, download this file as a .py file, go into the terminal, and run the following command:
# ```
# python3 pyspark-streaming/4_morestream/2_Integration_with_Kinesis_Demo.py
# ```
#     

# ### Demo

# In[ ]:


import findspark
# TODO: your path will likely not have 'matthew' in it. Change it to reflect your path.
findspark.init('/home/matthew/spark-2.3.0-bin-hadoop2.7')

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.3.0 pyspark-shell'


import sys
import json
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

appName="PythonKinesisApp"
sc = SparkContext(appName=appName)
ssc = StreamingContext(sc, 1)


streamName = 'DemoStream'
endpointUrl = 'https://kinesis.us-east-1.amazonaws.com'
regionName = 'us-east-1'
AWS_ACCESS_KEY_ID = ''
SECRET_ACCESS_KEY = ''
checkpointInterval = 5
kinesisstream = KinesisUtils.createStream(ssc, appName, 
                                    streamName, endpointUrl, regionName, 
                                    InitialPositionInStream.LATEST, 
                                    checkpointInterval, 
                                    awsAccessKeyId=AWS_ACCESS_KEY_ID, 
                                    awsSecretKey=SECRET_ACCESS_KEY)
lines = kinesisstream.map(lambda x: x[1])
counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
counts.pprint()

ssc.start()
time.sleep(600) # Run stream for 10 minutes just in case no detection of producer
# ssc.awaitTermination()
ssc.stop(stopSparkContext=True,stopGraceFully=True)


# ## References
# 1. https://spark.apache.org/docs/latest/streaming-kinesis-integration.html
# 2. https://spark.apache.org/docs/latest/streaming-programming-guide.html#performance-tuning

#  
