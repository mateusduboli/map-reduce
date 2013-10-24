__author__ 		= "Mateus Dubiela Oliveiar < first . last @ gmail.com>"
from mrjob.job import MRJob
import re

class TwitterFollowersCountApp(MRJob):


	def steps(self):
		return [
			self.mr(
				mapper=self.map_followers,
				reducer=self.reducer_sum),
			self.mr(
				mapper=self.map_group,
				combiner=self.reducer_greater),
				]
			
	def map_followers(self, key, value):
		user = int(value.split('\t')[0])
		yield (user, 1)
	
	def comb_username(self, key, value):


	def reducer_sum(self, key, values):
		yield (key, sum(values))

	def map_group(self, key, value):
		yield ("max",(value, key))

	def reducer_greater(self, key, values):
		n_follow, user = max(values)
		yield (user, n_follow) 	

if __name__ == '__main__':
	TwitterFollowersCountApp.run()
