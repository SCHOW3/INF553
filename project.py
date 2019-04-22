from __future__ import print_function

import sys
from operator import add
from collections import defaultdict

from pyspark.sql import SparkSession

def printf(p):
    print("p: ", list(p))

def create_user_movie(lists_of_list):
    to_return = defaultdict(list)
    for l in lists_of_list:
        to_return[int(l[0])].append(int(l[1]))
    return to_return

def create_movie_user(lists_of_list):
    to_return = defaultdict(list)
    for l in lists_of_list:
        to_return[int(l[1])].append(int(l[0]))
    return to_return

def get_movie_ids(movies):
    res = defaultdict()
    for movie in movies:
        res[int(movie[0])] = movie[1]
    return res


def calculate_hash(i, x):
    return (5*x + 13*i) % 100

def get_signatures(partition):
    # return list of lists which is the signatures
    signatures = []
    for user, movies in partition:
        user_signature = [100 for x in range(20)]
        for movie in movies:
            for i in range(20):
                hash_value = calculate_hash(int(i), int(movie))
                if(hash_value < user_signature[i]):
                    user_signature[i] = hash_value
        signatures.append((user, user_signature))
    print("signatures", signatures)
    return signatures

def convert_user_signatures_to_matrix(list_of_user_signatures):
    signature_matrix = [[0 for i in range(0, len(list_of_user_signatures))] for i in range(20)]
    for user, signatures in list_of_user_signatures:
        # Create the 20 X users matrix of signatures. 
        for signature_index in range(len(signatures)):
            signature_matrix[signature_index][user-1] = signatures[signature_index]
    return signature_matrix

def calculate_candidate_pairs(iterator):
    user_to_signatures = defaultdict(list)
    for signature_row in iterator:
        for user_id in range(len(signature_row)):
            user_to_signatures[user_id + 1].append(signature_row[user_id])
    # now i have a dictionary of user -> signature scores of each user in the band. I need to compare users to one another and look for matches
    candidate_pairs = []
    for key_index_outer in range(len(user_to_signatures) - 1):
        first_user = user_to_signatures[key_index_outer + 1]
        for key_index_inner in range(key_index_outer + 2, len(user_to_signatures) + 1):
            second_user = user_to_signatures[key_index_inner]
            if(first_user == second_user):
                candidate_pairs.append((key_index_outer + 1, key_index_inner))
                candidate_pairs.append((key_index_inner, key_index_outer + 1))
    return candidate_pairs


def calculate_jaccard_similarities(list_candidate_pairs, user_to_movie_dict):
    user_to_similar_users = defaultdict(list)
    for user, similar_users in list_candidate_pairs:
        # Need to calculate Jaccard of this user against all similar users
        for similar_user in similar_users:
            intersection = len(list(set(user_to_movie_dict[user]).intersection(user_to_movie_dict[similar_user])))
            union = (len(user_to_movie_dict[user]) + len(user_to_movie_dict[similar_user])) - intersection
            jaccard_similarity = float(intersection/union)
            user_to_similar_users[user].append((jaccard_similarity, similar_user))
        # Need to sort the list of similar users by jaccard_similarity_values
        user_to_similar_users[user] = sorted(user_to_similar_users[user])
    return user_to_similar_users

    
def find_recommendations(jaccard_similarities, user_to_movie_dict):
    user_list = jaccard_similarities.keys()
    user_list.sort()
    user_to_recommended_movies = defaultdict(list)
    for user in user_list:
        count_to_movie = defaultdict(list)
        movie_count = {}
        movie_count = defaultdict(lambda: 0, movie_count)
        similar_user_list = jaccard_similarities[user]
        for similar_user in similar_user_list:
            movies_watched = user_to_movie_dict[similar_user[1]]
            for movie in movies_watched:
                movie_count[movie] = movie_count[movie] + 1
            for movie, count in sorted(movie_count.items(), key = movie_count.get, reverse = True):
                count_to_movie[count].append(movie)
            count_to_movie_keys = count_to_movie.keys()
            count_to_movies_keys = sorted(count_to_movie_keys, reverse = True)
            for key in count_to_movies_keys:
                values_to_be_sorted = count_to_movie[key]
                values_sorted = sorted(values_to_be_sorted)
                for value in values_sorted:
                    if(len(user_to_recommended_movies[user]) < 3):
                        user_to_recommended_movies[user].append(value)
    return user_to_recommended_movies

def write_to_output_file(recommendation_list, output_file):
    f = open(output_file, "w+")
    # print("output_file", output_file)
    keylist = recommendation_list.keys()
    keylist.sort()
    for key in keylist:
        value_list = recommendation_list[key]
        value_string = ','.join([str(i) for i in value_list])
        output_string = "U"+str(key)+","+value_string
        # print("output_string: ", output_string)
        f.write("%s\n" % output_string)
    f.close()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: project.py ratings.csv movies.csv output.txt", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("project")\
        .getOrCreate()

    sc = spark.sparkContext

    user_to_movie_lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0]).map(lambda x: x.split(',')).collect()

    movie_lines = spark.read.text(sys.argv[2]).rdd.map(lambda r: r[0]).map(lambda x: x.split(',')).collect()
    # movies = get_movie_ids(movie_lines)
    # print(movies)
    user_to_movie_dict = create_user_movie(user_to_movie_lines)
    movie_to_user_dict = create_movie_user(user_to_movie_lines)
    # print(user_to_movie_dict)
    # print(movie_to_user_dict)

    # time to make the signature matrix
    parallized_movie_to_user = sc.parallelize(movie_to_user_dict.items(), 2)
    parallized_user_to_movie = sc.parallelize(user_to_movie_dict.items(), 2)
    # split into 2 partitions. 
    # for each partition run the signature matrix function
    user_signatures = parallized_user_to_movie.mapPartitions(get_signatures).sortByKey(True).collect()
    signature_matrix = convert_user_signatures_to_matrix(user_signatures)
    parallized_signature_bands = sc.parallelize(signature_matrix, 4)
    # I have now parallized the signature into 4 bands of 5 rows in each band. Each row contains all 11 users. 
    candidate_pairs = parallized_signature_bands.mapPartitions(calculate_candidate_pairs)\
        .distinct().groupByKey().sortByKey(True)\
        .collect()
    
    jaccard_similarities = calculate_jaccard_similarities(candidate_pairs, user_to_movie_dict)
    recommendation_list = find_recommendations(jaccard_similarities, user_to_movie_dict)

    write_to_output_file(recommendation_list, sys.argv[3])
    # print(recommendation_list)

    spark.stop()

