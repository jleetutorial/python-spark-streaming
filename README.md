# Python Spark Streaming

This repository contains the files and lectures for the _[Insert Title or Organization Name Here]_ Spark Streaming Tutorial using Python.

## Curriculum
### Section 1: Get started with Spark Streaming
1. Introduction to Streaming: 
	* What is streaming?
	* Why use streaming? 
	* Popular streaming tools (Kafka, Apache Spark streaming, etc.)
2. Overview of Apache Spark streaming
	* What is Apache Spark streaming?
	* The advantages of Apache Spark streaming
	* User case of Apache Spark streaming
3. Set up Environment on our local box: 
	* Install language SD, Python(if source code is written in Python) and Git ([Video Example](https://youtu.be/5wHfAfpHAjo))
	* Check out Source code
	* Setup IDE for our demo project
	* Use IntelliJ IDEA if the program code is written in Scala or Java
	* We will discuss what IDE to use if the code is written in Python ([Video Example](https://youtu.be/KDoZ6TsQHEg))
4. Run our First Spark streaming projet
	* The first project would just create a Sparkcontext connect to Twitter stream and print out the live stream tweets
	* Need to demo how to create a twitter developer account to the twitter oauth token
	* Need to set the logging level to ERROR to reduce the output noise ([Video Example](https://youtu.be/vLrcjVxdTng0))
	* Code samples [one](https://drive.google.com/open?id=0Bym8DZ5hyGifdmRKMVR1QVlKNW8) and [two](https://drive.google.com/open?id=0Bym8DZ5hyGifX2t3UXNHc0RpWDA), don’t copy and paste these
	* We should point out [winutils.exe needs to be installed for Windows users in order to run Spark applications](https://docs.google.com/document/d/1bAsB0ZBjXGQ4md0Z3LIeaHosPQ1eFUmiIdU_FdanPqE/edit).

### Section 2: Spark Streaming Basics
1. What are Discretized Streams
	* [Reference](https://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams)
	* Use some graph to explain DStreams
2. How to create Discretized Streams
	* Different ways to create DStreams
	* File Streams
	* Queue of RDDs as a Stream
	* Kafka
	* Flume
	* Kinesis
	* Twitter ([Reference](https://spark.apache.org/docs/latest/streaming-programming-guide.html#basic-sources), revisit your first Spark application)
	* **DEMO:** Queue of RDDs as a Stream
3. Transformations on DStreams
	* Basic RDD transformations(stateless transformation): `Map`, `flatMap`, `Filter`, `Repartition`, `Union`, `Count`, `Reduce`, `countByValue`, `reduceByKey`, `Join`, `Cogroup`
	* **DEMO:** Pick up 2 of the transformations to demo in the program
	* **EXERCISE:** prepare an exercise for student to use one of the transformations
4. Transform Operation
	* What is transform operation and the benefit of it ([Reference](https://spark.apache.org/docs/latest/streaming-programming-guide.html#transform-operation))
	* **DEMO:** do a demo with Transform Operation
	* **EXERCISE:** prepare an exercise for student to use transformation operation
5. Window Operations
	* What is Window Operations(better with some graphs)
	* Explain parameters (window length and sliding interval)
	* Some of the popular Window operations (e.g., `Window`, `countByWindow`, `reduceByKeyAndWindow`, `countByValueAndWindow`)
6. `Window`
	* Explain `Window` transformation in depth and what is the usage of `Window` function
	* **DEMO:** Do a demo with `Window` transformation
	* **EXERCISE:** Give an exercise about `Window` tansformation
7. `countByWindow`
	* Explain `countByWindow` transformation in depth and what is the usage of `countByWindow` function
	* **DEMO:** Do a demo with `countByWindow` transformation
	* **EXERCISE:** Give an exercise about `countByWindow` tansformation
8. `reduceByKeyAndWindow`
	* Explain `reduceByKeyAndWindow` transformation in depth and what is the usage of `reduceByKeyAndWindow` function
	* **DEMO:** Do a demo with `reduceByKeyAndWindow` transformation
	* **EXERCISE:** Give an exercise about `reduceByKeyAndWindow` tansformation
9. `countByValueAndWindow`
	* Explain `countByValueAndWindow` transformation in depth and what is the usage of `countByValueAndWindow` function
	* **DEMO:** Do a demo with `countByValueAndWindow` transformation
	* **EXERCISE:** Give an exercise about `countByValueAndWindow` tansformation
10. Output Operations on DStreams
	* Different output operation (e.g., `Print`, `saveAsTextFiles`, `saveAsObjectFiles`, `saveAsHadoopFiles`, `foreachRDD`)
	* **DEMO:** Demo how to save tweets to files ([Example](https://drive.google.com/open?id=0Bym8DZ5hyGifaXgwWFQxdVQ4UzA))
	* use `foreachRDD` and `saveAsTextFiles`
11. `foreachRDD`
	* Explain `foreachRDD` and the basic usage about `foreachRDD`
	* Design Patterns for `foreachRDD`
	* Reference: https://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd
	* **DEMO:** Do a demo with `foreachRDD`
	* **EXERCISE:** Give an exercise about `foreachRDD`
12. SQL OPERATIONS
	* [Dataframe and SQL Operations](https://spark.apache.org/docs/latest/streaming-programming-guide.html#dataframe-and-sql-operations)
	* **DEMO:** Do a demo with SQL OPERATIONS
	* **EXERCISE:** Give an exercise about SQL OPERATIONS

### 3. Section: Advanced
1. Join Operations
	* Different types of Join
	* Stream-stream joins
	* Stream-dataset joins
	* **DEMO:** Do a demo with Stream-stream joins
	* **DEMO:** Do a demo with Stream-dataset joins
	* **EXERCISE:** Give an exercise with Stream-stream joins or Stream-dataset joins
2. Stateful transformation
	* Transformations
	* `UpdateStateByKey`
	* `mapWithState`
	* **DEMO** Do a demo with `UpdateStateByKey` or `mapWithState`
	* Needs come up with a proper scenario to use `mapWithState` or `UpdateStateByKey`, such as some [web session data](https://drive.google.com/file/d/0Bym8DZ5hyGifWTJkQW5laUdwRU0/view).
	* **EXERCISE:** Prepare an exercise with UpdateStateByKey or mapWithState
3. Check point
	* What is checkpoint and why use check point
	* Different types of checkpoint (Metadata checkpointing & Data checkpointing)
	* When to enable Checkpointing
	* How to configure Checkpointing
	* **DEMO:** Do a demo with Checkpointing
	* **EXERCISE:** Give Exercise with Checkpointing
4. Accumulators
	* What is Accumulators and usage of Accumulators
	* **DEMO:** Do a demo with Accumulators
	* **EXERCISE:** Give an Exercise with Accumulators
5. Fault-tolerance
	* [Fault Tolerance Semantics](https://spark.apache.org/docs/latest/streaming-programming-guide.html#fault-tolerance-semantics)

### Section 4: More about Spark streaming
1. Performance Tuning
	* [Reference](https://spark.apache.org/docs/latest/streaming-programming-guide.html#performance-tuning)
	* Reducing the Batch Processing Times
	* Level of Parallelism in Data Receiving
	* Level of Parallelism in Data Processing
	* Data Serialization
	* Task Launching Overheads
	* Setting the Right Batch Interval
	* Memory Tuning
2. Integration with Kafka
	* Introduction to Kafka
	* Why integrate with Kafka
	* DEMO: [Demo](https://drive.google.com/file/d/0Bym8DZ5hyGifcnU1ZVVteEI3X1U/view?usp=drive_web)
3. Integration with Kinesis
	* Introduction to Kinesis
	* Why integrate with Kinesis
	* **DEMO:** [Demo](https://drive.google.com/file/d/0Bym8DZ5hyGifX2JNdFZENUpiRXM/view)

### Section 5: Structured Streaming
1. Introduction about Structured Streaming
	* Overview of Structured Streaming
	* [The Benefit of structured streaming](https://drive.google.com/file/d/0Bym8DZ5hyGifM2VOYlJVQ3NwaTg/view)
	* [Basic Concepts about Spark streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-concepts)
	* **DEMO:** [A quick demo about an structured streaming example](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#quick-example)
2. Operations on streaming DataFrames/Datasets
	* [Structured Streaming Programming Guide: Operations on Streaming Dataframe Datasets](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#operations-on-streaming-dataframesdatasets)
	* **DEMO:** DO a demo:
	* **EXERCISE:** Prepare an excise 
3. Window Operations
	* [Structured Streaming Programming Guide: Window Operations on Event Time](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time)
	* **DEMO:** Do a demo ([exmaple](https://drive.google.com/open?id=0Bym8DZ5hyGifU2YzUmx3aldVdkU))
	* **EXERCISE:** Prepare an excise 
4. Handling Late Data and Watermarking
	* [Handling Late Data and Watermarking Example](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking)

### Section 6: Finish up
1. Add an introductory lecture about that is covered in the course
	* This video should be placed as the first lecture of this course, but we do it after we are done creating this course
2. Add a promotion video
	* This will be about what users will learn from this lecture and how they will benefit
3. Finish up lecture
	* This last lecture to summarize what we have taught in this course and future learning material
