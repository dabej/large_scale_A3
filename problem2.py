from pyspark import SparkContext
import time
import numpy as np

t = []

INPUT_FILE = "/data/2020-DAT346-DIT873-TLSD/DATASETS/assignment3.dat"
for c in [1, 2, 4, 8, 16, 32]:
    t0 = time.time()
    sc = SparkContext(master=f'local[{str(c)}]')
    distFile = sc.textFile(INPUT_FILE)
    lines = distFile.map(lambda l: l.split())
    count = lines.count()
    sum_v = lines.map(lambda x: ("sum", float(x[2]))).reduceByKey(
        lambda x, y: x+y).collect()[0]
    mean = sum_v[1]/count
    std_dev = np.sqrt(lines.map(lambda x: (
        1, (float(x[2])-mean)**2)).reduceByKey(lambda x, y: x+y).collect()[0][1] / count)
    min_v = lines.map(lambda l: l[2]).min()
    max_v = lines.map(lambda l: l[2]).max()
    all_values = lines.map(lambda l: float(l[2])).collect()
    histogram_counts = np.histogram(all_values)[0].tolist()

    t1 = time.time()
    print(t1-t0)
    t.append(t1-t0)
    print("mean: " + str(mean))
    print("standard deviation: " + str(std_dev))
    print("min value: " + str(min_v))
    print("max value: " + str(max_v))
    print(histogram_counts)
    sc.stop()
print(t)
