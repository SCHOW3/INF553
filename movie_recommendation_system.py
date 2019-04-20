#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This is an example implementation of ALS for learning how to use Spark. Please refer to
pyspark.ml.recommendation.ALS for more conventional use.

This example requires numpy (http://www.numpy.org/)
"""
from __future__ import print_function

import sys
import pandas as pd
from rake_nltk import Rake
import numpy as np
from numpy.random import rand
from numpy import matrix
from pyspark.sql import SparkSession

LAMBDA = 0.01   # regularization
np.random.seed(42)


def rmse(R, ms, us):
    diff = R - ms * us.T
    return np.sqrt(np.sum(np.power(diff, 2)) / (M * U))


def update(i, mat, ratings):
    uu = mat.shape[0]
    ff = mat.shape[1]
#
    XtX = mat.T * mat
    Xty = mat.T * ratings[i, :].T
#
    for j in range(ff):
        XtX[j, j] += LAMBDA * uu
#
    return np.linalg.solve(XtX, Xty)

def load_dataframe(path):
    df = pd.read_csv("input/" + path)
    df = df[['budget', 'genres', 'homepage', 'id', 'keywords', 'original_language',\
        'original_title', 'overview', 'popularity', 'production_companies',\
        'production_countries', 'release_date', 'revenue', 'runtime',\
        'spoken_languages', 'status', 'tagline', 'title', 'vote_average', 'vote_count']]
    return df

def find_key_word(dataframe):
    dataframe["Key_words"] = ""
    return

if __name__ == "__main__":

    """
    Usage: als [M] [U] [F] [iterations] [partitions]"
    """

    print("""WARN: This is a naive implementation of ALS and is given as an
      example. Please use pyspark.ml.recommendation.ALS for more
      conventional use.""", file=sys.stderr)

    spark = SparkSession\
        .builder\
        .appName("movie_recommendation")\
        .getOrCreate()
    
    if len(sys.argv) != 2:
        print("usage: movie_recommendation_system.py <input_data>", file=sys.stderr)
        sys.exit(-1)

    sc = spark.sparkContext
    input_file = sc.textFile("input/data.csv")
    input_file_header = input_file.take(1)[0]
    print(input_file_header)

    # df = load_dataframe(sys.argv[1])
    
    # print(df.head())
    # print(df.size)
    # print(df.shape)
    
    spark.stop()
