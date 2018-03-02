__author__ = 'hanhanwu'

import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

conf = SparkConf().setAppName("ml pipeline")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

training_input = sys.argv[1]
testing_input = sys.argv[2]
output = sys.argv[3]


def main():
    # Read training data as a DataFrame
    sqlCt = SQLContext(sc)
    trainDF = sqlCt.read.parquet(training_input)
    testDF = sqlCt.read.parquet(testing_input)

    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    evaluator = BinaryClassificationEvaluator()

    # no parameter tuning
    hashingTF_notuning = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features", numFeatures=1000)
    lr_notuning = LogisticRegression(maxIter=20, regParam=0.1)
    pipeline_notuning = Pipeline(stages=[tokenizer, hashingTF_notuning, lr_notuning])
    model_notuning = pipeline_notuning.fit(trainDF)

    prediction_notuning = model_notuning.transform(testDF)
    notuning_output = evaluator.evaluate(prediction_notuning)

    # for cross validation
    hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
    lr = LogisticRegression(maxIter=20)

    paramGrid = ParamGridBuilder()\
        .addGrid(hashingTF.numFeatures, [1000, 5000, 10000])\
        .addGrid(lr.regParam, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])\
        .build()

    pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])
    cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=2)
    cvModel = cv.fit(trainDF)

    # Make predictions on test documents. cvModel uses the best model found.
    best_prediction = cvModel.transform(testDF)
    best_output = evaluator.evaluate(best_prediction)

    s = str(notuning_output) + '\n' + str(best_output)
    output_data = sc.parallelize([s])
    output_data.saveAsTextFile(output)

if __name__ == '__main__':
    main()
