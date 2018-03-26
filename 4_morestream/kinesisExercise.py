# This script will read 100 lines of data from PARQUET datasource stored in S3 and stream them into an AWS Kinesis stream.

from __future__ import print_function
import logging
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from boto import kinesis

logger = logging.getLogger('py4j')

kinesisStreamName='stream'

def write_partition(partition):
        # Access the Kinesis client object
        kinesisClient = kinesis.connect_to_region("us-east-1")

        # Iterate over rows
        for row in partition:
            # Send the row as a JSON string into the Kinesis stream
            kinesisClient.put_record(kinesisStreamName, json.dumps(row),"partitionKey")

if __name__ == "__main__":
    appName='Send2KinesisStream'

    sc = SparkContext()

    # Connect to the hive context of our spark context.
    sqlContext = SparkSession.builder.enableHiveSupport().getOrCreate();

    # Define an external hive table from the PARQUET files stored in S3 to be used as source to read data from.
    sqlContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS yellow_trips_parquet(" +
                    "pickup_timestamp BIGINT, vendor_id STRING, rate_code STRING, payment_type STRING) " +
                    #"ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
                     "STORED AS parquet " +
                    "LOCATION 's3://awskrug-jjy-athena/parquet/'")

    # Create an RDD containing 100 items from the external table defined above
    #lines=sqlContext.sql("select pickup_timestamp, vendor_id, rate_code, payment_type from yellow_trips_parquet limit 300")
    lines=sqlContext.sql("select * from yellow_trips_parquet limit 300")

	# Iterate over data
    lines.foreachPartition(write_partition)