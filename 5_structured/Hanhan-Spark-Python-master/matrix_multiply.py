__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
import numpy
import operator

conf = SparkConf().setAppName("matrix multiply")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

inputs = sys.argv[1]
output = sys.argv[2]
d = 10

# each RDD is the outer product of a column of A Transpose and the relative row of A
# each line input is a row from matrix A, it is also a row from the text input
def get_rdds(line):
    a = line.split()
    a = [float(num) for num in a]
    at = zip(a)
    single_out_product = numpy.multiply.outer(at, a)
    single_matrix = numpy.matrix(single_out_product)

    return single_matrix


def main():
    text = sc.textFile(inputs)

    out_product_rdds = text.map(lambda line: get_rdds(line))
    output_data = out_product_rdds.reduce(operator.add)
    output_matrix_arr = numpy.array(output_data)
    output_list = []
    for row_index in range(len(output_matrix_arr)):
        row = list(output_matrix_arr[row_index])
        row_str = ''
        for col_index in range(d):
            row_str += str(row[col_index]) + ' '
        output_list.append(row_str)

    output_data = sc.parallelize(output_list)

    output_data.saveAsTextFile(output)

if __name__ == '__main__':
    main()

