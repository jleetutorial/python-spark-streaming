__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
import numpy as np
from sklearn import svm
from sklearn import metrics
import matplotlib.pyplot as plt
from sklearn import preprocessing

conf = SparkConf().setAppName("Linear Kernel SVM")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

inputs = sys.argv[1]


def main():
    indata = np.load(inputs)
    training_data = indata['data_training']
    training_scaled = preprocessing.scale(training_data)
    training_labels = indata['label_training']
    validation_data = indata['data_val']
    validation_scaled = preprocessing.scale(validation_data)
    validation_labels = indata['label_val']
    ts = range(-12,6)
    cs = [pow(10, t) for t in ts]
    accuracy_results = []
    accuracy_results_scaled = []

    for c in cs:
        lin_clf = svm.LinearSVC(C=c)
        lin_clf.fit(training_data, training_labels)
        predictions = lin_clf.predict(validation_data)
        accuracy = metrics.accuracy_score(validation_labels, predictions)
        accuracy_results.append(accuracy)

        lin_clf.fit(training_scaled, training_labels)
        predictions = lin_clf.predict(validation_scaled)
        accuracy_scaled = metrics.accuracy_score(validation_labels, predictions)
        accuracy_results_scaled.append(accuracy_scaled)

    plt.plot(range(len(cs)), accuracy_results, label='un-scaled')
    plt.plot(range(len(cs)), accuracy_results_scaled, label='scaled')
    plt.xticks(range(len(cs)), cs, size='small')
    plt.legend()
    plt.show()
    print accuracy_results
    print accuracy_results_scaled

if __name__ == '__main__':
    main()
