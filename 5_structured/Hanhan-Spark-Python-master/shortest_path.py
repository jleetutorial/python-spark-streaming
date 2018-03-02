__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

conf = SparkConf().setAppName("shortest path")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
assert sc.version >= '1.5.1'

inputs = sys.argv[1]
output = sys.argv[2]
source_node = sys.argv[3]
dest_node = sys.argv[4]

textinput = sc.textFile(inputs)

def get_graphedges(line):
    list1 = line.split(':')
    if list1[1] == '':
        return None
    list2 = list1[1].split(' ')
    list2 = filter(None, list2)
    results = []
    s = list1[0]
    for d in list2:
        results.append((s, d))
    return results


KnownRow = Row('node', 'source', 'distance')

schema = StructType([
StructField('node', StringType(), False),
StructField('source', StringType(), False),
StructField('distance', IntegerType(), False),
])

graphedges_rdd = textinput.map(lambda line: get_graphedges(line)).filter(lambda x: x is not None).flatMap(lambda x: x).coalesce(1)
graphedges = graphedges_rdd.toDF(['source', 'destination']).cache()
graphedges.registerTempTable('SourceDestTable')

initial_node = source_node
initial_row = KnownRow(initial_node, initial_node, 0)
knownpaths = sqlContext.createDataFrame([initial_row], schema=schema)
part_knownpaths = knownpaths

for i in range(6):
    part_knownpaths.registerTempTable('PartKnownPathTable')

    newpaths = sqlContext.sql("""
    SELECT destination AS node, t1.source AS source, (distance+1) AS distance FROM
    SourceDestTable t1
    JOIN
    PartKnownPathTable t2
    ON (t1.source = t2.node)
    """)

    newpaths.registerTempTable('NewPathTable')
    knownpaths.registerTempTable('KnowPathTable')
    duplicate_df = sqlContext.sql("""
    SELECT t1.node AS node, t1.source as source, t1.distance as distance FROM
    NewPathTable t1
    JOIN
    KnowPathTable t2
    ON (t1.node = t2.node)
    """)

    if duplicate_df.count() != 0:
        newpaths = newpaths.subtract(duplicate_df)

    part_knownpaths = newpaths
    knownpaths = knownpaths.unionAll(newpaths)
    knownpaths.write.save(output + '/iter' + str(i), format='json')

knownpaths_rdd = knownpaths.rdd
knownpaths_map = knownpaths_rdd.map(lambda (node, source, distance): (node, (source, distance))).cache()

paths = []
paths.append(dest_node)
dest = knownpaths_map.lookup(dest_node)
for j in range(6):
    if not dest:
        paths = ['invalid destination']
        break
    parent = dest[0][0]
    paths.append(parent)
    if parent == source_node:
        break
    dest = knownpaths_map.lookup(parent)

paths = reversed(paths)

outdata = sc.parallelize(paths)
outdata.saveAsTextFile(output + '/path')
