from __future__ import print_function

import sys
from operator import add
from collections import defaultdict
import math
from pyspark.sql import SparkSession
import operator

def printf(p):
    print("p: ", list(p))

def create_user_movie(lists_of_list, user_data):
    to_return = defaultdict(list)
    for l in user_data:
        to_return[int(l[0]) + 1].append(int(l[1]))
    for l in lists_of_list:
        to_return[int(l[0]) + 1].append(int(l[1]))
    return to_return

def create_movie_user(lists_of_list, user_data):
    to_return = defaultdict(list)
    for l in user_data:
        to_return[int(l[1])].append(int(l[0]) + 1)
    for l in lists_of_list:
        to_return[int(l[1])].append(int(l[0]) + 1)
    return to_return

def create_user_movie_ratings(lists_of_list, user_data):
    to_return = defaultdict(dict)
    for l in user_data:
        to_return[int(l[0]) + 1][int(l[1])] = float(l[2])
    for l in lists_of_list:
        to_return[int(l[0]) + 1][int(l[1])] = float(l[2])
    return to_return

def get_movie_ids(movies):
    res = defaultdict()
    for movie in movies:
        # print(movie)
        res[int(movie[0])] = (",".join(movie[1][:-1]), list(movie[1][-1].split("|")))
    return res


def calculate_hash(i, x):
    return (5*x + 13*i) % 27000

def get_signatures(partition):
    # return list of lists which is the signatures
    signatures = []
    for user, movies in partition:
        user_signature = [sys.maxint for x in range(20)]
        for movie in movies:
            for i in range(20):
                hash_value = calculate_hash(int(i), int(movie))
                if(hash_value < user_signature[i]):
                    user_signature[i] = hash_value
        signatures.append((user, user_signature))
    # print("signatures", signatures)
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
    # print(candidate_pairs)
    return candidate_pairs

def calculate_genre_co_sim(user_A, user_B, movies_A, movies_B, movies):
    # calculate the jaccard based on the genres the movies are? do it per movie????
    genre_percentages = defaultdict(dict)
    genres = set()
    for movie in movies_A:
        for genre in movies[movie][1]:
            genres.add(genre)
            if "A" in genre_percentages[genre]:
                genre_percentages[genre]["A"] += 1
            else:
                genre_percentages[genre]["A"] = 1
    for movie in movies_B:
        for genre in movies[movie][1]:
            if "B" in genre_percentages[genre]:
                genre_percentages[genre]["B"] += 1
            else:
                genre_percentages[genre]["B"] = 1
    # print("number of moviesA, B:", len(movies_A), len(movies_B), 1/len(movies_A), 1/len(movies_B))
    # print("genre_percentages", genre_percentages)
    # print("# of genres", len(genres))
    # print("# of keys", len(genre_percentages.keys()))
            # print(genre)
    cos_sin_numer = 0.0
    cos_sin_denom_A = 0.0
    cos_sin_denom_B = 0.0
    for genre, dic in genre_percentages.items():
        if len(dic.keys()) == 2:
            a_contribution = float(dic["A"]*100/len(movies_A))
            b_contribution = float(dic["B"]*100/len(movies_B))
            # print("contributions", a_contribution, b_contribution)
            cos_sin_numer += a_contribution * b_contribution
        for user, count in dic.items():
            if user == "A":
                cos_sin_denom_A += (count*100/len(movies_A))**2
            else:
                cos_sin_denom_B += (count*100/len(movies_B))**2
    denom = math.sqrt(cos_sin_denom_A) * math.sqrt(cos_sin_denom_B)
    cos_sim_genre = cos_sin_numer/denom
    # print(cos_sim_genre)
    return cos_sim_genre

def calc_cosine_similarity(list_candidate_pairs, user_movie_ratings, movies):
    # user_to_similar_users = defaultdict(list)
    user_to_potential_sim = defaultdict(list)
    for A, similar_users in list_candidate_pairs:
        for B in similar_users:
            A_dict = user_movie_ratings[A]
            B_dict = user_movie_ratings[B]
            A_sum = 0.0
            B_sum = 0.0
            overlap_sum_num = 0.0
            for movie, rating in A_dict.items():
                if movie in B_dict:
                    overlap_sum_num += rating * B_dict[movie]
                A_sum += rating**2
            for movie, rating in B_dict.items():
                B_sum += rating**2
            denom = float(math.sqrt(A_sum)) * float(math.sqrt(B_sum))
            cos_sim = overlap_sum_num/denom
            # Jaccard Similarity of the movie genres get added here. 
            genre_sim = calculate_genre_co_sim(A, B, A_dict.keys(), B_dict.keys(), movies)
            user_to_potential_sim[A].append((B, cos_sim+genre_sim))
        pairs = user_to_potential_sim[A]
        pairs.sort(key=lambda tup: tup[1], reverse=True)
        user_to_potential_sim[A] = pairs
    return user_to_potential_sim

def find_recommendations(cosine_sim, user_to_movie_dict):
    # print(user_to_movie_dict)
    users = cosine_sim.keys()
    users.sort()
    result = defaultdict(list)
    # for A in users:
    A = users[0]
    print("A", A)
    print("B", cosine_sim[A])
    movies_watched = {}
    for B in cosine_sim[A]:
        for movie in user_to_movie_dict[B[0]]:
            if movie in movies_watched:
                movies_watched[movie] += B[1] + 1
            else:
                movies_watched[movie] = B[1] + 1
    
    sorted_movies = sorted(movies_watched.items(), key=operator.itemgetter(1))
    sorted_movies.reverse()
    movies_watched_by_a = user_to_movie_dict[A]
    if len(sorted_movies) > 10:
        for movie in sorted_movies:
            if movie not in movies_watched_by_a:
                result[A].append(movie)
    else:
        for movie in sorted_movies:
            if movie not in movies_watched_by_a:
                result[A].append(movie)

    return result

def create_user_to_movie_names_list(recommendation_list, movies):
    result = defaultdict(list)
    for user, movie_id_list in recommendation_list.items():
        for movie_id, score in movie_id_list:
            movie_name = movies[movie_id]
            result[user].append(movie_name)
    return result

def write_signature_matrix(signature_matrix):
    f = open("signature_output.txt", "w+")
    f.write("signature matrix \n")
    f.write("Dimension: "  + str(len(signature_matrix)) + " " + str(len(signature_matrix[0])))
    for signature in signature_matrix:
        f.write("%s\n" %signature)
    f.close()

def write_to_output_file(recommendation_list, output_file):
    f = open(output_file, "w+")
    keylist = recommendation_list.keys()
    keylist.sort()
    for key in keylist:
        movies = recommendation_list[key]
        for movie in movies:
            f.write(movie[0].encode("utf-8") + "\n")
        # movies_string = ', '.join([movie.encode("utf-8") for movie in movies])
        # output_string = "User "+str(key)+": "+movies_string
        # f.write("%s\n" % output_string)
    f.close()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: project.py ratings.csv movies.csv output.txt user_input.txt", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("project")\
        .getOrCreate()

    sc = spark.sparkContext

    user_movie_input = spark.read.text(sys.argv[4]).rdd.map(lambda r: r[0]).map(lambda x: x.split(',')).collect()
    user_to_movie_lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0]).map(lambda x: x.split(',')).collect()
    movie_lines = spark.read.text(sys.argv[2]).rdd.map(lambda r: r[0]).map(lambda x: x.encode("ascii", "ignore").split(',', 1)).map(lambda x: (x[0], x[1].split(',', -1))).collect()
    movies = get_movie_ids(movie_lines)

    user_to_movie_dict = create_user_movie(user_to_movie_lines, user_movie_input)
    movie_to_user_dict = create_movie_user(user_to_movie_lines, user_movie_input)
    user_movie_ratings = create_user_movie_ratings(user_to_movie_lines, user_movie_input)

    # time to make the signature matrix
    parallized_movie_to_user = sc.parallelize(movie_to_user_dict.items(), 2)
    parallized_user_to_movie = sc.parallelize(user_to_movie_dict.items(), 2)
    # split into 2 partitions. 
    # for each partition run the signature matrix function
    user_signatures = parallized_user_to_movie.mapPartitions(get_signatures).sortByKey(True).collect()
    signature_matrix = convert_user_signatures_to_matrix(user_signatures)
    # print("############ Signautre_Matrix", len(signature_matrix), len(signature_matrix[0]))
    write_signature_matrix(signature_matrix)
    # print(signature_matrix)
    parallized_signature_bands = sc.parallelize(signature_matrix, 4)
    # I have now parallized the signature into 4 bands of 5 rows in each band. Each row contains all 11 users. 
    candidate_pairs = parallized_signature_bands.mapPartitions(calculate_candidate_pairs)\
        .distinct().groupByKey().sortByKey(True)\
        .collect()

    cosine_sim = calc_cosine_similarity(candidate_pairs, user_movie_ratings, movies)
    # print("cosine sim")
    # print(cosine_sim)
    # recommendation_list = find_recommendations(cosine_sim, user_to_movie_dict)
    recommendation_list = find_recommendations(cosine_sim, user_to_movie_dict)
    user_to_movie_names_list = create_user_to_movie_names_list(recommendation_list, movies)
    # print(user_to_movie_names_list)
    write_to_output_file(user_to_movie_names_list, sys.argv[3])
    # print(recommendation_list)

    spark.stop()

