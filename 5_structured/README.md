# Spark Structured Streaming with a Static Input
An application that takes as input the list of twitter user IDs and every 5 seconds, it emits the number of tweet actions of a user if it is present in the input list.
An inner join on the staticDF consisting of twitterIDs and the input stream DF was performed and grouped on the userIDs.

## Dataset
This application was developed to analyze the Higgs Twitter Dataset. The Higgs dataset has been built after monitoring the spreading processes on Twitter before, during and after the announcement of the discovery of a new particle with the features of the Higgs boson. Each row in this dataset is of the format <userA, userB, timestamp, interaction> where interactions can be retweets (RT), mention (MT) and reply (RE).
We have split the dataset into a number of small files so that we can use the dataset to emulate streaming data. Download the split dataset onto your master VM.

## streamer.sh
This script emulates twitter stream by doing the following :
* Copies the entire split dataset to the HDFS. This would be the staging directory.
* Creates a monitoring directory on the HDFS that this application listens to. This would be the directory this streaming application is listening to.
* Periodically, moves the split dataset files from the staging directory to the monitoring directory using the hadoop fs -mv command.

## Usage
Submit this spark job by using the following command : 

`spark-submit --verbose tweetactions.py <path_to_monitoring_dir_in_hdfs>`