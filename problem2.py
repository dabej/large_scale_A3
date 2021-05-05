from pyspark import SparkContext
from pyspark.sql import SparkSession
import time

if __name__ == '__main__':
    t0 = time.time()

    INPUT_FILE = "1000.dat"

    spark = SparkSession.builder.master("local[2]") \
        .appName('SparkByExamples.com') \
        .getOrCreate()
    # sc = spark.sparkContext()  # (master='local[4]')
    rdd = spark.sparkContext.textFile(INPUT_FILE)
    res = rdd.map(lambda l: l.split("\t")).map(
        lambda t: ("key", 1)).reduceByKey(lambda a, b: a+b).count()
    print(res)
    t1 = time.time()
