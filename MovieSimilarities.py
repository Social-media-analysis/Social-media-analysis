# To run locally:
# !python MovieSimilarities.py --items=ml-100k/u.item ml-100k/u.data > sims.txt

# To run on a single EMR node:
# !python MovieSimilarities.py -r emr --items=ml-100k/u.item ml-100k/u.data

# To run on 4 EMR nodes:
#!python MovieSimilarities.py -r emr --num-ec2-instances=4 --items=ml-100k/u.item ml-100k/u.data

# Troubleshooting EMR jobs (subsitute your job ID):
# !python -m mrjob.tools.emr.fetch_logs --find-failure j-1NXMMBNEQHAFT


from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt
# this for computing all permutations of the movie pairs
from itertools import combinations

class MovieSimilarities(MRJob):

    def configure_options(self):
        super(MovieSimilarities, self).configure_options()
        self.add_file_option('--items', help='Path to u.item')

    def load_movie_names(self):
        # Load database of movie names.
        self.movieNames = {}
        with open("u.item", encoding='ascii', errors='ignore') as f:
            # line
            # 1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
            for line in f:
                fields = line.split('|')
                self.movieNames[int(fields[0])] = fields[1]
    """
    step 1:
    creating movie rating pairs by user
    step 2:
    computing similarities between every possible movie combination
    step 3:
    sort it and make it into an output
    """
    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_input,
                    reducer=self.reducer_ratings_by_user),
            MRStep(mapper=self.mapper_create_item_pairs,
                    reducer=self.reducer_compute_similarity),
            MRStep(mapper=self.mapper_sort_similarities,
                    mapper_init=self.load_movie_names,
                    reducer=self.reducer_output_similarities)]


    def mapper_parse_input(self, key, line):
        # Outputs userID => (movieID, rating)
        # 196   242 3   881250949
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield  userID, (movieID, float(rating))  # 296, (242, 3)

    # same key go here
    def reducer_ratings_by_user(self, user_id, itemRatings): # 296, (242,3),(485,4)...
        #Group (item, rating) pairs by userID
        ratings = []
        for movieID, rating in itemRatings:
            ratings.append((movieID, rating)) #[(242,3),(485,4)...]

        yield user_id, ratings  # 296, [(242,3),(485,4)...]



    def mapper_create_item_pairs(self, user_id, itemRatings): # 296, [(242,3),(485,4)...]
        # Find every pair of movies each user has seen, and emit
        # each pair with its associated ratings

        # "combinations" finds every possible pair from the list of movies
        # this user viewed.
        # combinations('ABCD', 2) --> AB AC AD BC BD CD
        for itemRating1, itemRating2 in combinations(itemRatings, 2): # find all possible (242,3),(485,4)
            # itemRating1 = (242,3)
            # itemRating2 = (485,4)
            movieID1 = itemRating1[0] # 242
            rating1 = itemRating1[1] # 3
            movieID2 = itemRating2[0] # 485
            rating2 = itemRating2[1] # 4

            # Produce both orders so sims are bi-directional
            yield (movieID1, movieID2), (rating1, rating2) # (242,485),(3,4)
            yield (movieID2, movieID1), (rating2, rating1) # (485,242),(4,3)


    def cosine_similarity(self, ratingPairs):
        # Computes the cosine similarity metric between two
        # rating vectors.
        numPairs = 0
        sum_xx = sum_yy = sum_xy = 0
        for ratingX, ratingY in ratingPairs: # ratingPairs is (3,4),(3,4),(3,3)...
            # ratingX is 3
            # ratingY is 4
            sum_xx += ratingX * ratingX # ratingX * ratingX is 9
            sum_yy += ratingY * ratingY # ratingY * ratingY is 16 
            sum_xy += ratingX * ratingY # ratingX * ratingY is 12
            numPairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))
        return (score, numPairs) # (similarity, number of users who saw both)

    def reducer_compute_similarity(self, moviePair, ratingPairs): # (242,485),(3,4),(3,4),(3,3)...
        # Compute the similarity score between the ratings vectors
        # for each movie pair viewed by multiple people
        # Output movie pair => score, number of co-ratings
        score, numPairs = self.cosine_similarity(ratingPairs) # ratingPairs is (3,4),(3,4),(3,3)...
        # Enforce a minimum score and minimum number of co-ratings
        # to ensure quality
        if (numPairs > 10 and score > 0.95):
            # (242,485),(similarity, number of users who saw both)
            yield moviePair, (score, numPairs) # (242,485), (0.98,99)

    def mapper_sort_similarities(self, moviePair, scores): # (242,485), (0.98,99), (0.99,129)...
        # Shuffle things around so the key is (movie1, score)
        # so we have meaningfully sorted results.
        score, n = scores # (0.98,99)
        # score = 0.98
        # n = 99
        movie1, movie2 = moviePair # (242,485)

        # Shuffle things around so the key is (movie1, score)
        # (movie_name1, 0.98),(movie_name2, 99)
        yield (self.movieNames[int(movie1)], score), \
            (self.movieNames[int(movie2)], n)

    def reducer_output_similarities(self, movieScore, similarN): # (movie_name1, 0.98),(movie_name2, 99), (movie_name3,87) ....
        # Output the results.
        # Movie => Similar Movie, score, number of co-ratings
        movie1, score = movieScore # (movie_name1, 0.98)
        for movie2, n in similarN: # similarN is (movie_name2, 99), (movie_name3,87) ....
            yield movie1, (movie2, score, n) # movie_name1, (movie_name2, 0.98, 99)


if __name__ == '__main__':
    MovieSimilarities.run()



