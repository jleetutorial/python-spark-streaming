__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

conf = SparkConf().setAppName("temp range")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
assert sc.version >= '1.5.1'

inputs = sys.argv[1]
output = sys.argv[2]

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

df = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='false') \
    .load(inputs, schema=temp_schema)
df = df.filter(df.QFlag == '')

dfmax = df.select(df['StationID'], df['DateTime'], df['Observation'], df['DataValue']).where(df['Observation'] == 'TMAX')
dfmax = dfmax.withColumnRenamed('Observation', 'MaxObservation')
dfmax = dfmax.withColumnRenamed('DataValue', 'MaxDataValue')

dfmin = df.select(df['StationID'], df['DateTime'], df['Observation'], df['DataValue']).where(df['Observation'] == 'TMIN')
dfmin = dfmin.withColumnRenamed('Observation', 'MinObservation')
dfmin = dfmin.withColumnRenamed('DataValue', 'MinDataValue')

cond1 = [dfmax['StationID'] == dfmin['StationID'], dfmax['DateTime'] == dfmin['DateTime']]
dfrange = dfmax.join(dfmin, cond1, 'inner').select(dfmax['StationID'], dfmax['DateTime'], (dfmax['MaxDataValue']-dfmin['MinDataValue']).alias('Range'))

df_maxrange = dfrange.groupby('DateTime').agg({"Range": "max"})
df_maxrange = df_maxrange.withColumnRenamed('max(Range)', 'MaxRange')

cond2 = [dfrange['DateTime'] == df_maxrange['DateTime'], dfrange['Range'] == df_maxrange['MaxRange']]
df_result = dfrange.join(df_maxrange, cond2, 'inner').select(dfrange['DateTime'], dfrange['StationID'], df_maxrange['MaxRange'])

results = df_result.rdd.map(lambda r: str(r.DateTime)+' '+str(r.StationID)+' '+str(r.MaxRange))
outdata = results.sortBy(lambda r: r[0]).coalesce(1)

outdata.saveAsTextFile(output)





