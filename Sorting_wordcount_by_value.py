from pyspark import SparkConf,SparkContext

import re
#importing re to use regular expression

conf = SparkConf().setMaster("local").setAppName("Wordcount")
sc = SparkContext( conf = conf)

def normalizeWords(text):
	return re.compile(r'\W+').split(text.lower())

lines = sc.textFile("book.txt")
words = lines.flatMap(normalizeWords)
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y)
wordCountsSorted = wordCounts.map(lambda (x,y):(y,x)).sortByKey()
results = wordCountsSorted.collect()

for count,word in results:
	if(word):
		print word,count
