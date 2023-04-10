# 영화 레이팅 COUNT하고 COUNT 순으로 정렬
from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesBreakdown(MRJob):
	def steps(self):
        return [
        	MRStep(mapper=self.mapper_get_ratings,
            	   reducer=self.reducer_count_ratings),
		    MRStep(reducer=self.reducer_sorted_output)
        ]
	
	def mapper_get_ratings(self, _, line):
    	(userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1
	
    def reducer_count_ratings(self, key, values):
    	yield str(sum(values)).zfill(5), key # STREAMING은 모든 것을 문자열로 인식, STREAMING에서 JSON을 사용하면 타입이 보존
	
    def reducer_sorted_output(self, count, movies):
		for movie in movies:
			yield movie, int(count)

if __name__=='__main__':
	MoviesBreakdown.run()
	
# (local) python MoviesBreakdown.py u.item 

""" (hadoop cluster)
python MoviesBreakdown.py
-r hadoop 
--hadoop-streaming-jar {hadoop-streaming.jar 위치}
{data 위치}
"""