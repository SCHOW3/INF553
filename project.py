from __future__ import print_function

import sys
import os
import pandas as pd
from operator import add
from collections import defaultdict

from pyspark.sql import SparkSession
# from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: movie_recommendation_system.py <input_data>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="project")

    raw_data = sc.textFile(sys.argv[1])
    header = raw_data.take(1)[0]
    data = raw_data.filter(lambda line: line != header)\
        .map(lambda line: line.split(","))

    # Now we need to Identify Pairs?
    print(type(data))

    # print(data.take(3))

    
    df.show()

