__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


conf = SparkConf().setAppName("temp range sql")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
assert sc.version >= '1.5.1'

inputs1 = sys.argv[1]
output = sys.argv[2]


def get_range(recordings):
    recordings.registerTempTable('Recordings')

    dfrange = sqlContext.sql("""
    SELECT r1.DateTime, r1.StationID, (r1.DataValue-r2.DataValue) AS Range FROM
    (SELECT StationID, DateTime, Observation, DataValue FROM Recordings
     WHERE Observation='TMAX') r1
     JOIN
     (SELECT StationID, DateTime, Observation, DataValue FROM Recordings
     WHERE Observation='TMIN') r2
     ON (r1.StationID = r2.StationID AND r1.DateTime = r2.DateTime)
    """)
    dfrange.registerTempTable('RangeTable')

    df_maxrange = sqlContext.sql("""
    SELECT DateTime, MAX(Range) AS MaxRange FROM RangeTable
    GROUP BY DateTime
    """)
    df_maxrange.registerTempTable('MaxRange')

    df_result = sqlContext.sql("""
    SELECT t1.DateTime as DateTime, t1.StationID as StationID, t2.MaxRange as MaxRange FROM
    RangeTable t1
    JOIN MaxRange t2
    ON (t1.DateTime = t2.DateTime AND t1.Range = t2.MaxRange)
    """)
    return df_result


def main():
    temp_schema = StructType([
    StructField('StationID', StringType(), False),
    StructField('DateTime', StringType(), False),
    StructField('Observation', StringType(), False),
    StructField('DataValue', DoubleType(), False),
    StructField('MFlag', StringType(), True),
    StructField('QFlag', StringType(), True),
    StructField('SFlag', StringType(), True),
    StructField('OBSTime', StringType(), True),
    ])

    df = sqlContext.read.format('com.databricks.spark.csv').options(header='false').load(inputs1, schema=temp_schema)
    df = df.filter(df.QFlag == '')

    dfrange = get_range(df)
    result = dfrange.rdd.map(lambda r: str(r.DateTime)+' '+str(r.StationID)+' '+str(r.MaxRange))
    outdata = result.sortBy(lambda r: r[0]).coalesce(1)
    outdata.saveAsTextFile(output)

if __name__ == "__main__":
    main()
