__author__ = 'hanhanwu'

import numpy as np
from pyspark import SparkConf, SparkContext
import matplotlib.pyplot as plt
import sys
import caffe
import os
import glob


conf = SparkConf().setAppName("image classification")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

caffe_root = sys.argv[1]
images_folder_path = sys.argv[2]   
output_path = sys.argv[3]


def vis_square(data):
    data = (data - data.min()) / (data.max() - data.min())

    n = int(np.ceil(np.sqrt(data.shape[0])))
    padding = (((0, n ** 2 - data.shape[0]),
               (0, 1), (0, 1))
               + ((0, 0),) * (data.ndim - 3))
    data = np.pad(data, padding, mode='constant', constant_values=1)

    data = data.reshape((n, n) + data.shape[1:]).transpose((0, 2, 1, 3) + tuple(range(4, data.ndim + 1)))
    data = data.reshape((n * data.shape[1], n * data.shape[3]) + data.shape[4:])
    f = plt.figure()
    plt.imshow(data); plt.axis('off')


def main():
    sys.path.insert(0, caffe_root + 'python')
    caffe.set_mode_cpu()
    model_def = caffe_root + 'models/bvlc_reference_caffenet/deploy.prototxt'
    model_weights = caffe_root + 'models/bvlc_reference_caffenet/bvlc_reference_caffenet.caffemodel'

    net = caffe.Net(model_def,
                    model_weights,
                    caffe.TEST)

    mu = np.load(caffe_root + 'python/caffe/imagenet/ilsvrc_2012_mean.npy')
    mu = mu.mean(1).mean(1)

    transformer = caffe.io.Transformer({'data': net.blobs['data'].data.shape})

    transformer.set_transpose('data', (2,0,1))
    transformer.set_mean('data', mu)
    transformer.set_raw_scale('data', 255)
    transformer.set_channel_swap('data', (2,1,0))

    net.blobs['data'].reshape(50,
                              3,
                              227, 227)

    # display image name, class number, label
    image_predictions = []
    for image_path in glob.glob(images_folder_path+'*.jpg'):
        image = caffe.io.load_image(image_path)
        transformed_image = transformer.preprocess('data', image)
        net.blobs['data'].data[...] = transformed_image
        output = net.forward()
        output_prob = output['prob'][0]
        pred = output_prob.argmax()

        labels_file = caffe_root + 'data/ilsvrc12/synset_words.txt'
        labels = np.loadtxt(labels_file, str, delimiter='\t')
        lb = labels[pred]

        image_name = image_path.split(images_folder_path)[1]

        result_str = image_name +'    '+lb
        image_predictions.append(result_str)

    pred_rdd = sc.parallelize(image_predictions)
    pred_rdd.saveAsTextFile(output_path)

if __name__ == '__main__':
    main()
