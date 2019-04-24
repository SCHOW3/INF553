def get_print_statement(genre):
    if genre == 1:
        print("Pick 3 movies from that genre:\n1. Neighbors\n2. Meet the Fockers\n3. Bridesmaids\n4. 40-Year-Old Virgin\n5. The Hangover\n6. The Step Brothers\n7. Mean Girls\n8. Zoolander")
    elif genre == 2:
        print("Pick 3 movies from that genre:\n1. Star Wars 2\n2. Star Wars 3\n3. Spiderman\n4. Terminator\n5. Interstellar\n6. The Avengers\n7. Jurassic Park\n8.")
    elif genre == 3:
        print("Pick 3 movies from that genre:\n1. The Grudge\n2. It\n3. The Shining\n4. The Conjuring\n5. Alien\n6. Friday the 13th\n7. Pscyho\n8. Anabelle\n9. The Exorcist")
    elif genre == 4:
        print("Pick 3 movies from that genre:\n1. The Notebook\n2. Titanic\n3. Gone With The Wind\n4. The Princess Bride\n5. Her\n6. Love Actually\n7. Crazy Stupid Love\n8. Twilight\n9.Fault In Our Stars")
    elif genre == 5:
        print("Pick 3 movies from that genre:\n1. Guardians of the Galaxy\n2. The Dark Knight\n3. Iron Man\n4. Inception\n5. Dune\n6. Thor\n7. The Matrix\n8. Gladiator\n9. Road House")

if __name__ == "__main__":
    movie_data = [
        [111113, 30825, 86833, 35836, 69122, 60756, 7451, 4816], # comedy
        [5378, 33493, 5349, 1240, 109487, 89745, 480], # starwars 2, starwars 3, spiderman, terminator, insterstellar, the avengers, jurassic park
        [8947, 26693, 1258, 103688, 1214, 66783, 1219, 114713, 1997], # grudge, it, shining, conjuring, alien, friday the 13th, psycho, Annabelle, The Exorcist
        [8533, 1721, 920, 1197, 106920,6942, 88163, 63992, 111921], # The Notebook, Titanic, Gone With The Wind, The Princess Bride, Her, Love Actually, Crazy Stupid Love, Twilight, Fault In Our Stars
        [112852, 58559, 59315, 79132, 2021, 86332, 2571, 3578] # Guardians of the Galaxy, The Dark Knight, Iron Man, Inception, Dune, Thor, The Matrix, Gladiator, Road House
    ]
    genre1 = input("Pick a genre:\n1. Comedy\n2. Sci-Fi\n3. Horror\n4. Romance\n5. Action\n")
    movies = list()
    get_print_statement(genre1)
    for i in range(3):
        n = input()
        movies.append(movie_data[genre1 - 1][n - 1])
    genre2 = input("Pick another genre:\n1. Comedy\n2. Sci-Fi\n3. Horror\n4. Romance\n5. Action\n")
    movies2 = list()
    get_print_statement(genre2)
    for i in range(3):
        n = input()
        movies.append(movie_data[genre2 - 1][n - 1])
    genre3 = input("Pick another genre:\n1. Comedy\n2. Sci-Fi\n3. Horror\n4. Romance\n5. Action\n")
    movies3 = list()
    get_print_statement(genre3)
    for i in range(3):
        n = input()
        movies.append(movie_data[genre3 - 1][n - 1])
    f = open("user_input.txt", "w+")
    for movie in movies:
        f.write("0," + str(movie) + ",5\n")
    f.close()
