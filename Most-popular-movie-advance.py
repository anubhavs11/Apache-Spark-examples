# INPUT FILE
#http://files.grouplens.org/datasets/movielens/ml-100k.zip

from pyspark import SparkConf,SparkContext

def loadMovieNames():
	movieNames={}
	with open("ml-100k/u.item") as f:
		for line in f:
			fields = line.split('|')
			movieNames[int(fields[0])]=fields[1]
	return movieNames
	
conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext( conf = conf)

nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("ml-100k/u.data")
movies = lines.map(lambda x : (int(x.split()[1]),1))

movieCount = movies.reduceByKey(lambda x,y:x+y)

flipped = movieCount.map(lambda x:(x[1],x[0]))
res = flipped.sortByKey()

sortedMoviesWithNames = res.map(lambda x: (nameDict.value[x[1]],x[0]))

results = sortedMoviesWithNames.collect()

for movie,popularity in results:
	print movie,popularity
