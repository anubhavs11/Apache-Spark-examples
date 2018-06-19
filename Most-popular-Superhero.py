from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext( conf = conf)

def parseName(line):
	fields = line.split('\"')
	return (int(fields[0]),fields[1])

def countOccurences(line):
	elements = line.split()
	return (int(elements[0]),len(elements)-1)

movie_names = sc.textFile("Marvel_Names.txt")
movies = movie_names.map(parseName)

movie_data = sc.textFile("Marvel_Graph.txt")
pairings = movie_data.map(countOccurences)

total = pairings.reduceByKey(lambda x,y:x+y)

flipped = total.map(lambda (x,y):(y,x))

result = flipped.max()
mostPopularSuperhero = movies.lookup(result[1])[0]
print(mostPopularSuperhero+" is the Most Popular Superhero with "+str(result[0])+" co-apearances.")
