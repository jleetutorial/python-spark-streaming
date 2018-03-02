# Hanhan-Spark-Python
Spark Python code I have wrote here was <b>using Spark1.5</b>, may not apply to later version all the time.

* <b>To run Spark in different situations</b>, check SFU guidance: https://courses.cs.sfu.ca/2016fa-cmpt-732-g5/pages/RunningSpark


wordcount-improved.py and reddit-averages.py

These 2 files are doing the same works like WordCountImproved.java, RedditAverages.java in my Hadoop-MapReduce folder, but instead of using java MapReduce, it's using Spark core python to do MapReduce works


correlate-logs.py
This file does the same work as CorrelateLogs.java I wrote in HBase-MapReduce-in-Java folder, but instead of using java MapReduce, I am using Spark core python.


correlate-logs-better.py
This file does the same work as correlate-logs.py but it is using a better formula which is mroe numerical stable https://courses.cs.sfu.ca/2015fa-cmpt-732-g1/pages/Assignment3B


itemsets.py

This file is using Spark MlLib FP-Growth (a Frequent Pattern Mining algorithm) to find the top 10,000 item sets.
The transaction data input is from here: http://fimi.ua.ac.be/data/T10I4D100K.dat


relative-score.py

This file calculates the best comment on Redit, it is looking for the largest values of (comment score)/(subreddit average score)


relative-score-bcast.py

This file does the similar work as relative-scire.py, but deals with huge data input. When the data is huge and join is expensive, it is using Spark core python broadcast join. Once you broadcast a value, it is sent to each executor. You can access it by passing around the broadcast variable and any function can then get the .value property out of it.

To calculate the relative score, we map over the comments: write a function that takes the broadcast variable and comment as arguments, and returns the relative average as before. This technique is a broadcast join.


 reddit_average_sql.py
 
 This file does the same work like RedditAverage.java in my Hadoop-MapReduce folder but instead of using java, it is using Spark SQL.
 
 
 load_logs_sql.py
 
 This file does the same work as LoadLogs.java in my Hadoop-MapReduce folder but instead of using java, it is using Spark SQL.
 
 temp_range.py, temp_range_sql.py
 
 These 2 files look at look at historical weather data from the Global Historical Climatology Network. Their archives contain (among other things) lists of weather observations from various weather stations formatted as CSV files like this:

USC00242347,20120105,TMAX,133,,I,K,0800

USC00242347,20120105,TMIN,-56,,,K,0800

USC00242347,20120105,TOBS,-56,,,K,0800

US1SCPC0003,20120105,PRCP,0,,,N,

US1SCPC0003,20120105,SNOW,0,,,N,

NOE00133566,20120105,TMAX,28,,,E,

NOE00133566,20120105,TMIN,-26,,,E,

NOE00133566,20120105,PRCP,57,,,E,

NOE00133566,20120105,SNWD,0,,,E,

The fields are the weather station; the date (Jan 5 2012 in these lines); the observation (min/max temperature, amount of precipitation, etc); the value (and integer); and four more columns we won't use. The readme file in the archive explains the fields in detail. We will worry about the min and max temperature (TMIN, TMAX) which are given as °C × 10.

The problem they are trying to solve is: what weather station had the largest temperature difference on each day? That is, where was the largest difference between TMAX and TMIN?

temp_range.py is using Spark core python while temp_range_sql.py is using Spark sql.

 
 shortest_path.py
 
 This file is using Spark Sql to calculate Dijkstra's algorithm - Shortest Path.
 
The data input will represent the graph by listing the outgoing edges of each node (and all of our nodes will be labelled with integers):

1: 3 5

2: 4

3: 2 5

4: 3

5:

6: 1 5
 
 
read_stream.py

This file work with Spark Streaming to process a constant stream of data in real(-ish) time.

In order to produce some streaming data, I have set up a simple TCP server on cmpt732.csil.sfu.ca that returns (x,y) points as plain text lines like this:

103.492239 100.839233

883.152476 878.051790

-331.287403 -333.856794

The the server is simple: it generates random points near a line. This will simulate a stream of sensor data.

It does a simple linear regression on the data and store the slope (which the Wikipedia page calls α̂ ) and intercept (β̂ ) of the values. https://en.wikipedia.org/wiki/Simple_linear_regression


movie_recomendations.py

Recommended movies using Spark MLlib Collaborative Filtering. To register the user-entered titles with Tweetings, use the Tweetings title with the smallest Levenshtein distance from the user's entry. There is a Levenshtein implementation in pyspark.sql

Sample user's ratings:

10 The Lord of the Rings: The Return of the King

8 The Lord of the Rings: The Fellowship of the Ring (2001)

5 orrest Gump (1994)

9 Mad Max: Fury Road (2014)

3 Mad Max (1979)

The dataset is from here: https://github.com/sidooms/MovieTweetings


euler.py

This file does the same work as what I did in Hadoop-MapReduce folder, EvaluateEuler.java, but instead of using java MapReduce, it is using python sql.

*********************************************************************************
Spark MLlib and Spark ML

1. matrix_multiply.py
 * Check data input in data_matrix.txt. 
 Note: The original input data is huge and could not be uploadded to GitHub. The code is also good for n rows data input when n is huge.
 * The code in matrix_multiply.py is using Spark python to calculate transpose(A)*A, A is a matrix (input).
 * Instead of using the traditional way to calculate matric multiply, here I am using the theory of outer product, https://en.wikipedia.org/wiki/Matrix_multiplication#Outer_product
 * It means, no matter how large n (row) will be, if the number of columns of a matrix can be put on a single machine, we can just parallely use an individual column of tranpose(A) to multiply the relative row of A, and finally add all the matrics up.

2. matrix_multiply_sparse.py
 * The code here is using the same idea for matrix multiply, but is desiged for handling sparse matrix, by taking advantage of python csr_matrix.

3. spark_ml_pipline.py
 * The code in this python file is to get to know Spark machine learning pipeline.
 * Using cross validation to tune parameters, getting the best prediction model.
 
Sentiment Analysis
1. amazon_review_tfidf.py, amazon_review_tfidf_normalized.py
 * Get bag of words and using tfidf scores as the feature vector.
 * amazon_review_tfidf_normalized.py added normalizer.

2. tfidf_cv_lowestRMSE.py, tfidf_cv_lowestRMSE_normalized.py
 * The input is the output from amazon_review_tfidf.py, amazon_review_tfidf_normalized.py.
 * Using cross validation, linear regression model to get the lowest RMSE and the relative step size.
 * For tfidf_cv_lowestRMSE_normalized.py, it is using L2 as the regularization type.

3. word2vec.py, word2vec_kmeans.py, word2vec_best_RMSE.py, word2vec_histogram_best_RMSE.py
 * Instead of using tfidf to generate feature vectors for linear regression model, we can use Word vector. word2vec.py shows how to generate the model.
 * After generating the word2vec model, I am using kmeans to generate the clusters for the data. Check word2vec_kmeans.py.
 * word2vec_best_RMSE.py is using word2vec generated feature vectors and linear regression model to get the best RMSE. Here I am not doing cross validation, since the code takes very long time, in order to avoid overfitting, it is becessary to do cross validation.
 * word2vec_histogram_best_RMSE.py is using the clusters generated by word2vec.py to create a histogram for each review, then use the normalized histogram as feature vector for linear regression model.
   * Details for creating the histogram for each review:
     * There are 2000 clusters in total. Each review has 70 words.
     * Each review has 1 histogram, and for this histogram, it is list with 2000 elements. The elements all started from 0, each element represent the count for each cluster. For example, if a word belongs to cluster 0, the first element in the list will add 1.
     * Normalize the histogram: calcuate the sum of 2000 element counts, and then each element count is divided by the sum.
     * Finally return the histogram for each review as a SparseVector.
   
   
4. GradientBoostedTrees.py, RandomForests.py
 * Instead of using linear regression model, the code is trying to use Greadient Boosted Trees and Random Forests to do the prediction. The average RMSE is much lower than the experiments in previous code. But in practical, need to add cross validation in the code here.

5. Different approached to improve the sentiment analysis model:
 * Different features: We could try different encoding techniques instead of bag-of-words. For example, VLAD 2 or Fisher encoding 3 or a concatenation of TF-IDF with word2vec features.
 * Different models: We could use different regression models sich as a random forest regression model.
 * Exploring model parameters: We saw that the size of word2vec features can affect its accuracy at the cost of a longer training time.


Anomalies Detection
1. anomalies_detection_data_sample.txt
 * This this the input data sample. The real data input is in parquet format so that it can be changed into DataFrame once we have read the data.

2. anomalies_detection.py
 * In the code, I didn't use RDD in cat2Num() and addScore() since the transformation between RDD and DataFrame in Spark is expensive. With DataFrame udf and withColumn(), we can do many operations in a convenient way.
 * As your can see in the data input, each row is a feature vector, the first 2 columns are not numerical data, we need to change the 2 columns into one-hot key representation. one-hot is using 1 to represent the distance between each 2 value pair.
 * After we have trained the data into clusters by using kMeans, we need to calculate confidence score, score = (N_max - N_x)/(N_max - N_min). N_max and N_min are the sizes of the largest and the smallest clusters, while N_x represent each cluster's size.
 * Finally we filter put those points have confidence scores higher than the threshold, they will be the anomalies.
 Note:
 * Here the formular I calculate the confidence score looks quite similar to the way I did normalization, in my machine learning projects. But they are 2 things.
There are 3 popular ways to do numerical feature rescaling:
 * Rescaling: y = (x-min(x))/(max(x)-min(x))
 * Standardization: y = (x-avg(x))/delta
 * Scaling to unit length: y = x/||x||
 * At a high level, Rescaling and Standardization are doing column-wise scaling (i.e., taking a column as input and output scaled column); the normalization is doing row-wise scaling (i.e., taking a row as input and output scaled row). To decide which one to use, it really depends on your data. For example, if you have a feature vector for a person: (140lb, 1.80m, 29 years old). It would be better to do column-wise scaling first and then do row-wise scaling since features are in different metrics. But for the feature vector of bag-of-words, only doing row-wise scaling should be enough.
 * In Spark, StandardScalar is to avoid results to be dominated by some features. For example, in KMeans, if you don't do feature scaling, the final distance will be dominated by those features whose range of values are very large. Regularization is to avoid model overfitting. It adds model complexity into the objective function of linear regression.

3. streaming_anomalies_detection.py
 * This code is for real time anomalies detection, since Spark has not released streaming DataFrame, I have spent lots of time to convert the DStream into DataFrame so that the later operations should be easier, but so far, still didn't get the expected results. (In developing....)


Data Cleaning - Entity Resolution

* Some Papers recommended by my professor:
 1. Efficient similarity search and similarity query: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.357.2270&rep=rep1&type=pdf

* entity_resolution.py
 1. ER is defined as finding different records that refer to the same real-world entity, e.g., iPhone 4-th generation vs. iPhone four. It is central to data integration and cleaning. In this assignment, you will learn how to apply ER in a data integration setting. But the program that you are going to write can be easily extended to a data-cleaning setting, being used to detect duplication values
 2. I'm using Jaccard Similarity to calculate the similarities, and using Inverted Index for the join here has significantly improved the efficiency.
 3. By joining the 2 Inverted Index here, we assumed that similar items should share at least 1 word (non-stopword) in their names.
 4. Implementation Details:
    * each item has an id, and split each of their names into a a list of string, remove the stop words, the rest of the words will be used as the joinKey for this item.
    * By using flatMap() method, the (id, joinKey) pair will be change to a list of (token, id) pairs, and this list is the Inverted Index! Join the 2 lists of Inverted Interd, remove the duplicates. And by selecting the ids in both lists from the joined result, those origional joinKeys which do not share even a single word will be automatically removed.
    * After using Inverted Index to do the initial filtering, now we need to use Jaccard Similarity to do further filtering. Item pairs which pass the threshold will remain. When using Jaccard Similarity here, I had to make sure the lists used in the formula all only contain distinct tokens, otherwise the result cannot be accurate.
    * Finally, I am uising precision, recall and fmeasure to measure the accuracy with the ground truth.


Movie Recommendation

* The dataset is from MovieLens 100K data, the training data combined its all training data, and the testing data combined its all testing data.

* There are several popular operations in movie recommendations: problem formulation, content-based filtering, collaborative filtering (memory-based, model-based), rating data

1. als.py
  * This model can be found in Spark mllib. It is a model-based model in collaborative filtering.

2. slope_one.py
  * This model is a memory-based model in collaborative filtering.
  * I implemented its logic in the code. And you will find using DataFrame is far more better than using rdd in this case.
  * Firstly, using the training data to calculate the deviation between each movie pairs. dev(j,i) = sum(r_j-r_i)/c(j,i),   [Here, j, i are movie ids, r_j, r_i are ratings provided by a user for both jth and ith movie, c(j,i) if the size of a set of users that rated both movies].
  * Secondly, calculate how likely a user in the testing set will like a movie. This value P(a,j) is in range [1,5], so when we are comparing this value of the ground truth, no need to do more works. P(a,j) = sum((dev(j,i)+r(a,i))*c(j,i))/sum(c(j,i)),  [Here, dev, r, c all come from the training data results. jth movie here has not been rated by user a in the training data].

3. More notes about Collaborative Filtering and Building Recommendation System (using movie recommendatio as an example)
  * Problem formaulation: build a nested dictionary first, if the key if a user, then the value of each key is the products rated by the user; if the key is a product, then the value for each key is a list of user who have rated this product.
  * Content-based filtering has more drawbacks than collaborative filtering (otherwise why do we use collaborative filtering more often). The major drawback is difficult to get clean feature vectors, such as it is not clear what the features should be, or it is expensive to create the vectors, or it does not scale well with the number of movies.
  * Collaborative Filtering - Memory-based, Model-based
  * Momory-based majorly using Item-based, User-based and Slope One. They can be used to learn similarity betweeen uers and predict unknown rating/find similar users; learn the similary between all items and do prediction/recommendation.
  * Compare item-based and user-based, we always have much more users than the amount of products, so item-based similarity is less expensive to compute and store.
  * But both item-based and user-based methods are heavily depend on the similarity measure, in order to solve this problem, we can use Slope One, since it computes deviations between item pairs. But it still suffers from data sparsity.
  * Model-based - ALS (Alternative Least Squares), learn models for users and items. it is trying to minimize P*Transpose(Q)-R, P=user feature matrix, Q=item feature matrix, R=rating matrix. It is similar to linear regression, but both P, Q are unknown.

4. Pytho Graphlab has built-in recommender if you want to use collaborative filtering, makes life easier: http://www.analyticsvidhya.com/blog/2016/06/quick-guide-build-recommendation-engine-python/?utm_source=feedburner&utm_medium=email&utm_campaign=Feed%3A+AnalyticsVidhya+%28Analytics+Vidhya%29
 


SVM

1. linear_svm.py, kernelized_svm.py, random_forest_with_bagging.py
  * The data is from Kaggle Competition, Forest-Cover-Type-Prediction.
  * I am using linear svm, kernelized svm and random forest with bagging as its sample selection method.
  Note: During the experiments, the kernelized svm took very long time when I was changing different parameters to work on the same data sets.


Deep Learning

* Using Caffe for deep learning
* Using Spark to distribute the work on multi-core.
1. image_classification.py
  * The training data came form Caffe example images, then classify the new images using other dataset.
2. model_visualization.py
  * visualize neural network models in each deep learning layer.


