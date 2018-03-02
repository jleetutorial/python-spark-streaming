__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
import numpy as np
from sklearn import svm
from sklearn import metrics
import matplotlib.pyplot as plt

conf = SparkConf().setAppName("Kernelized SVM")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

inputs = sys.argv[1]


def main():
    indata = np.load(inputs)
    training_data = indata['data_training']
    training_labels = indata['label_training']
    validation_data = indata['data_val']
    validation_labels = indata['label_val']
    ts = range(-12,6)
    cs = [pow(10, t) for t in ts]
    gs = range(-8, 0)
    gammas = [pow(10, g) for g in gs]

    for gm in gammas:
        legend_label = 'gamma='+str(gm)
        accuracy_results = []
        for c in cs:
            clf = svm.SVC(C=c, kernel='rbf', gamma=gm)
            clf.fit(training_data, training_labels)
            predictions = clf.predict(validation_data)
            accuracy = metrics.accuracy_score(validation_labels, predictions)
            accuracy_results.append(accuracy)
        plt.plot(range(len(cs)), accuracy_results, label=legend_label)

    plt.xticks(range(len(cs)), cs, size='small')
    plt.legend()
    plt.show()

if __name__ == '__main__':
    main()
