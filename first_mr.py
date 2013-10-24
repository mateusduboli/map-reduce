__author__ 		= "Mateus Dubiela Oliveiar < first . last @ gmail.com>"
from mrjob.job import MRJob


class FirstMRJobApp(MRJob):

	def steps(self):
		return [self.mr(
			mapper=self.map_identity,
			reducer=self.reducer_identity),]
			
	def map_identity(self, key, value):
		yield (key, value)

	def reducer_identity(self, key, values):
		for v in values:
			yield (key,v)

if __name__ == '__main__':
	FirstMRJobApp.run()
