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

    spark = SparkSession.builder.appName('recommender').getOrCreate()
    df = spark.read.csv("tmdb_5000_movies.csv", inferSchema=True, header=True)
    # df = df.select(['id', 'title', 'genres', 'keywords', 'popularity'])
    # df = df.loc([keywords])
    df = df.select('title', 'genres', 'keywords', 'popularity')
    
    df.show()

