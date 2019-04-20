from __future__ import print_function

import sys
import os
import pandas as pd
from operator import add
from collections import defaultdict

from pyspark.sql import SparkSession
# from pyspark import SparkContext


# if __name__ == "__main__":
#     if len(sys.argv) < 1:
#         sys.exit(-1)

#     sc = SparkContext(appName="project")

#     # raw_data = sc.textFile("tmdb_5000_movies.csv")
#     df = sc.read.csv("tmdb_5000_movies.csv", inferSchema=True, header=True)
#     df.printSchema()
#     # header = raw_data.take(1)[0]
#     # data = raw_data.filter(lambda line: line != header)\
#     #     .map(lambda line: line.split(","))

#     # print(data.collect())
    

if __name__ == "__main__":
    spark = SparkSession.builder.appName('recommender').getOrCreate()
    df = spark.read.csv("tmdb_5000_movies.csv", inferSchema=True, header=True)
    # df = df.select(['id', 'title', 'genres', 'keywords', 'popularity'])
    # df = df.loc([keywords])
    df = df.select('title', 'genres', 'keywords', 'popularity')
    
    df.show()

