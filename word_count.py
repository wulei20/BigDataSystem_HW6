import re
import sys
from pyspark import SparkConf, SparkContext
import time

K = 10

if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])

    first = time.time()

    # Word Count
    word_counts = lines.flatMap(lambda line: re.split('[^\w]+', line)) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1], ascending=False)
    
    # Top K
    top_k = word_counts.take(K)
    for word, count in top_k:
        print("('%s', %i)" % (word, count))

    last = time.time()

    print("Total program time: %.2f seconds" % (last - first))
    sc.stop()
