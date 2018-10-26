from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_get_ratings,reducer=self.reducer_count_ratings),
                MRStep(reducer = self.reducer_find_max)]


    def mapper_get_ratings(self, _, line):
        # 196  242 3   881250949
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1 #   242,1  302,1  242,1  51,1

#This mapper does nothing; it's just here to avoid a bug in some
#versions of mrjob related to "non-script steps." Normally this
#wouldn't be needed.
    # def mapper_passthrough(self, key, value):
    #     yield key, value

    def reducer_count_ratings(self, key, values):  # 242,1,1    302,1   51,1
        yield None, (sum(values), key) # None,(2,242)   None,(1,302)   None,(1,51)

    # this func will be called one time
    def reducer_find_max(self, key, values): # None,(2,242) ,(1,302) ,(1,51),.....
        yield max(values) # 2,242

if __name__ == '__main__':
    MostPopularMovie.run()
