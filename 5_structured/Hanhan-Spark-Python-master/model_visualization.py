__author__ = 'hanhanwu'


import numpy as np
from pyspark import SparkConf, SparkContext
import matplotlib.pyplot as plt
import sys
import caffe
import os


conf = SparkConf().setAppName("model visualization")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'

caffe_root = sys.argv[1]


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

    image = caffe.io.load_image(caffe_root + 'examples/images/cat.jpg')
    transformed_image = transformer.preprocess('data', image)
    net.blobs['data'].data[...] = transformed_image
    output = net.forward()
    output_prob = output['prob'][0]
    print 'predicted class is:', output_prob.argmax()

    # The models in blobs will change based on the input picture, the models in the params will not change
    lst_blobs = [e for e in net.blobs.keys() if e.startswith('conv') == True]
    for i in range(len(lst_blobs)):
        filters = net.blobs[lst_blobs[i]].data
        sp = filters.shape
        col1 = sp[0]
        col2 = sp[1]
        col3 = sp[2]
        col4 = sp[3]
        vis_square(filters.reshape(col1*col2, col3, col4))

    lst_params = [e for e in net.params.keys() if e.startswith('conv') == True]
    for i in range(len(lst_params)):
        filters = net.params[lst_params[i]][0].data
        sp = filters.shape
        col1 = sp[0]
        col2 = sp[1]
        col3 = sp[2]
        col4 = sp[3]
        vis_square(filters.reshape(col1*col2, col3, col4))

    plt.show()

if __name__ == '__main__':
    main()
