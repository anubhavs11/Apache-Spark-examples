from pyspark.sql import SparkSession,Row

import collections

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
def getMovieName():
	movie_dict={}
	with open("ml-100k/u.item") as f:
		for line in f:
			fields = line.split("|")
			movie_id = int(fields[0])
			name = fields[1]
			movie_dict[movie_id] = name;
	return movie_dict

movie_names = getMovieName()

def mapper(data):
	fields = data.split()
	return Row(movie=int(fields[1]))

data = spark.sparkContext.textFile("ml-100k/u.data")
movies = data.map(mapper)

movie_data = spark.createDataFrame(movies)

topMovieIDs = movie_data.groupBy("movie").count().orderBy("count",ascending = False).cache()
top10=topMovieIDs.take(20)

for movies in top10:
	print(movie_names[movies[0]]+" : "+movie_names[1])

spark.stop()
	
