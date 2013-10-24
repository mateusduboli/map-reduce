__author__ 		= "Mateus Dubiela Oliveiar < first . last @ gmail.com>"
from mrjob.job import MRJob
import re

class TwitterFollowersCountApp(MRJob):


	def steps(self):
		return [self.mr(
			mapper=self.map_followers,
			reducer=self.reducer_sum),]
			
	def map_followers(self, key, value):
		user, data = value.split('\t',1)
		user = int(user)
		r = re.compile('^\d+$')
		if(r.match(data)):
			yield (user,{"follower":1})	
		else:
			yield (user, {"username":data})

	def reducer_sum(self, key, values):
		p = sum([v for k, v in values if k == 'follower'])
		h = (v for k, v in values if k == 'username')
		yield (h[0],p)

if __name__ == '__main__':
	TwitterFollowersCountApp.run()
