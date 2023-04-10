# 영화 점수 COUNT
from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
	def steps(self):
        return [
        	MRStep(mapper=self.mapper_get_ratings,
            	   reducer=self.reducer_count_ratings)
        ]
	def mapper_get_ratings(self, _, line):
    	(userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1
    def reducer_count_ratings(self, key, values):
    	yield key, sum(values)

if __name__='__main__':
	RatingsBreakdown.run()
	
# (local) python RatingsBreakdown.py u.item 

""" (hadoop cluster)
python RatingsBreakdown.py
-r hadoop 
--hadoop-streaming-jar {hadoop-streaming.jar 위치}
{data 위치}
"""