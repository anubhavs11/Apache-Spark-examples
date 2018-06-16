# INPUT FILE
#http://files.grouplens.org/datasets/movielens/ml-100k.zip

from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext( conf = conf)

lines = sc.textFile("ml-100k/u.data")
movies = lines.map(lambda x : (x.split()[1],1))

movieCount = movies.reduceByKey(lambda x,y:x+y)

flipped = movieCount.map(lambda x:(x[1],x[0]))
res = flipped.sortByKey()
results = res.collect()

for popularity,movie in results:
	print movie,popularity
