I was using Spark 1.5. Now Spark 2.0 has make many changes and in fact it should be easier to use and faster for big data operations. Here, I'm creating this new folder, recording my Spark 2.0 practicing experience

**************************************************************************

INSTALL & RUN SPARK 2.0

* If you want to install/run it in different situations, such as in your local machine, Hadoop VMs, Cluster, etc. Check [this SFU CS Big Data Guidance][1]
* <b>NOW! You finally can simply install Spark through pip!</b>, type `pip install pyspark` in your terminal. I tried, now I can use spark in my IPython much easier
* <b>OR, An Old Method - To add Spark in PyCharm</b>
  1. I'm using [PyCharm][2], it is great
  2. [Download Spark][3], at the time I'm wrinting this guidance, I am using `spark-2.1.1-bin-hadoop2.7`
  3. Open your PyCharm, create a new project. Open PyCharm Preference, and find `Project Structure`. Click `Add Content Root`, and find the path to `[your spark download folder]/spark-2.1.1-bin-hadoop2.7/python/lib`, add `py4j-0.10.4-src.zip`, `pyspark.zip`. Then you should be able to use Spark in your PyCharm
* To run your .py file that's using Spark
  * Even if you have Spark in IDE, still need to run it through terminal. So, open your terminal
  * Type `export SPARK_HOME=[your spark download folder path]/spark-2.1.1-bin-hadoop2.7`
  * Type `${SPARK_HOME}/bin/spark-submit --master local [your python file].py [input file 1]`, input file is optional, all depends on how you are going to read the data input in your code. And you can set multiple input files


**************************************************************************

Anomalies Detection (offline vs Streaming)

* I'm planning to test Spark2.0 streaming, to see whether it can make real time data analysis, and hope to find a way to check model quality
* Anomalies detection, OFFLINE (without streaming)
  * [Data Sample - parquet files][4]
  * [What does sample data look like][9], since you cannot read parquet directly
  * [My Code: Spark 2.0 Anomalies Detection code - With OO Design][5]
    * Compared with Spark1.5, one of the major changes is, `SqlContext` has been replaced with `SparkSession`, in the code we call it as `spark`. Meanwhile, `spark context` can be got from `spark.sparkContext`. If I didn't remember wrong, the reason they made this change is to make calling spark sql easier. You can simple use created `spark` to do many things that originally needed more libraries
  * [My Code: Spark 2.0 Anomalies Detection - IPython][11] - more clear to see input and output
    * [Same Data Input as above - parquet files][4]
    * Uses k-means clustering, then clusters with less member counts (higher score, higher than a threshold) will be considered as anomalies

* Anomalies detection, with streaming
  * When it comes to real time detection experiments, you can try these methods with Spark
    * [Spark Streaming][6] - Apply Spark streaming machine learning methods on streaming data
    * [Structured Streaming][7] - This one is still in Alpha Experimental stage, they are trying to allow you use streaming just like offline spark code
    * Offline trained model for online data - If your coming data do not have significant changes, then train your model with historical data offline, and apply this model on online data, but need to check your model quality periodically, to make sure it still works fine
    
  * Experiment 0 - Spark Streaming Basics
    * [Local Network Connection][13]
    
  * Experiment 1 - Spark Streaming
    * [How Spark Streaming K-Means Work][12]
      * For each batch of data, it applies 2 steps of k-means
        * Random initialize k centers
        * Generate clusters, compute the average for each cluster and update the new centers
      * [My Code: Spark streaming k-means][14]
        * [Sample training data][15]
        * [Sample testing data][16]
        * With `predictOnValues`, you need label and features, and you need `LabelPoint(label, feature_vector)` to put them together, vector has to be `Vectors.dense(python_list)`
        * With `predictOn`, you don't need the label, but just to have `Vectors.dense(python_list)` to put features together
        * dimension in `setRandomCeters()` means the number of features in a row
      * [My Code: Spark Streaming Anomalies Detection][17]
        * [Same Data Input as above Offline Anomalies Detection - parquet files][4]
        * [What does sample data look like][9], since you cannot read parquet directly
    * [Spark streaming k-means example][8]
    * [streaming k-means built-in methods][10]



[1]:https://courses.cs.sfu.ca/2016fa-cmpt-732-g5/pages/RunningSpark
[2]:https://www.jetbrains.com/pycharm/download/#section=mac
[3]:https://spark.apache.org/downloads.html
[4]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/logs-features-sample.zip
[5]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/anomalies_detection.py
[6]:https://spark.apache.org/docs/2.1.0/streaming-programming-guide.html#overview
[7]:https://spark.apache.org/docs/2.1.0/structured-streaming-programming-guide.html
[8]:http://spark.apache.org/docs/latest/mllib-clustering.html#streaming-k-means
[9]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/anomalies_detection_data_sample.txt
[10]:http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.clustering.StreamingKMeans
[11]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/Saprk_anomalies_detction.ipynb
[12]:https://databricks.com/blog/2015/01/28/introducing-streaming-k-means-in-spark-1-2.html
[13]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/spark_streaming_word_count.ipynb
[14]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/spark_kmeans_streaming.ipynb
[15]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/kmeans_train.csv
[16]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/kmeans_test.csv
[17]:https://github.com/hanhanwu/Hanhan-Spark-Python/blob/master/Spark2.0/spark_streaming_anomalies_detection.ipynb
