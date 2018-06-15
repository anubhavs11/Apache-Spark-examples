from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("Wordcount")
sc = SparkContext( conf = conf)

lines = sc.textFile("book.txt")
words = lines.flatMap(lambda x : x.split())
results = words.countByValue()

for word, count in results.items():
	cleanWord = word.encode('ascii','ignore')
	if(cleanWord):
		print cleanWord,count
