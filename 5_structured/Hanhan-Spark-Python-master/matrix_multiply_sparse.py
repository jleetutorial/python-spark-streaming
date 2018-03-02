__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
import numpy
from scipy.sparse import *
from scipy import *
import operator

conf = SparkConf().setAppName("matrix multiply sparse")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

inputs = sys.argv[1]
output = sys.argv[2]
s = 100

def get_single_outproduct(line):
    elems = line.split()
    a = [float(e.split(':')[1]) for e in elems]
    row_indexs = [float(e.split(':')[0]) for e in elems]
    at = zip(a)
    single_out_product = numpy.multiply.outer(at, a)
    dense_matrix = numpy.matrix(single_out_product)

    return dense_matrix, row_indexs


def to_s2_matrix(dense_matrix, row_indexes):
    dense_arr = numpy.array(dense_matrix)
    dense_arr = [a for arr in dense_arr for a in arr]
    s2_matrix = numpy.zeros((s,s))
    count = 0

    for i in row_indexes:
        for j in row_indexes:
            s2_matrix[i][j] = dense_arr[count]
            count += 1

    return s2_matrix



def main():
    text = sc.textFile(inputs)
    t = text.map(lambda line: get_single_outproduct(line))
    outproducts = t.map(lambda (dense_matrix, row_indexes): to_s2_matrix(dense_matrix, row_indexes))
    output_matrix = outproducts.reduce(operator.add)
    output_matrix_arr = numpy.array(output_matrix)
    output_list = []
    for row_index in range(len(output_matrix_arr)):
        row = list(output_matrix_arr[row_index])
        row_str = ''
        for col_index in range(s):
            row_str += str(col_index) + ':' + str(row[col_index]) + ' '
        output_list.append(row_str)


    output_data = sc.parallelize(output_list)

    output_data.saveAsTextFile(output)

if __name__ == '__main__':
    main()
