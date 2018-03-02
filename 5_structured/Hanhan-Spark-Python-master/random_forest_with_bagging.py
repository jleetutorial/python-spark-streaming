__author__ = 'hanhanw'

import sys
from pyspark import SparkConf, SparkContext
import numpy as np
from sklearn import ensemble
from sklearn import metrics
import matplotlib.pyplot as plt

conf = SparkConf().setAppName("Random Forest using Bagging")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

inputs = sys.argv[1]


def main():
    indata = np.load(inputs)
    training_data = indata['data_training']
    training_labels = indata['label_training']
    validation_data = indata['data_val']
    validation_labels = indata['label_val']
    ts = range(1,11)
    sampling_rates = [round(0.1*t, 1) for t in ts]
    forest_sizes = [10, 20, 50, 100]


    for sampling_rate in sampling_rates:
        legend_label = 'sampling rate='+str(sampling_rate)
        accuracy_results = []
        for forest_size in forest_sizes:
            rf_clf = ensemble.BaggingClassifier(n_estimators=forest_size, max_samples=sampling_rate)
            rf_clf.fit(training_data, training_labels)
            predictions = rf_clf.predict(validation_data)
            accuracy = metrics.accuracy_score(validation_labels, predictions)
            accuracy_results.append(accuracy)
        plt.plot(range(len(forest_sizes)), accuracy_results, label=legend_label)

    plt.xticks(range(len(forest_sizes)), forest_sizes, size='small')
    plt.legend()
    plt.show()

if __name__ == '__main__':
    main()
