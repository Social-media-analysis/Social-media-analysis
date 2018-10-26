from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):

    """
    this is how we tell mrjob that we have additional options,
    that we want to accept on the command line when we run this script
    In this case, we are calling add_file_option to indicate that
    there is another ancillary file that we want to send along with every nodes
    after --items, pass the file name, and file will be distrubuted to every node
    """
    def configure_options(self):  # --items u.item
        super(MostPopularMovie, self).configure_options()
        self.add_file_option('--items', help='Path to u.item')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper = self.mapper_passthrough,
                   reducer = self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    # this run before reducer
    def reducer_init(self):
        # in this case, key is movieID, value is movie name
        self.movieNames = {}  
        with open("u.ITEM", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split('|')
                # this allows us  later on do a quick look up of the name for a movie id
                self.movieNames[fields[0]] = fields[1]  # {'movie_id': 'movie_name'}

    def reducer_count_ratings(self, key, values): # movieID, 1,1,1 ....
        yield None, (sum(values), self.movieNames[key]) # None, (3,movie_name)

#This mapper does nothing; it's just here to avoid a bug in some
#versions of mrjob related to "non-script steps." Normally this
#wouldn't be needed.
    def mapper_passthrough(self, key, value):
        yield key, value

    def reducer_find_max(self, key, values): # None, (3,movie_name), (...) ....
        yield max(values)  # (3, movie_name)...

if __name__ == '__main__':
    MostPopularMovie.run()


# execute this file by
# python MostPopularMovieNicer.py --items=u.ITEM u.data


