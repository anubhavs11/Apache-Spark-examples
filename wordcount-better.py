from pyspark import SparkConf,SparkContext

import re
#importing re to use regular expression

conf = SparkConf().setMaster("local").setAppName("Wordcount")
sc = SparkContext( conf = conf)

def normalizeWords(text):
	return re.compile(r'\W+').split(text.lower())

lines = sc.textFile("book.txt")
words = lines.flatMap(normalizeWords)
results = words.countByValue()

for word, count in results.items():
	cleanWord = word.encode('ascii','ignore')
	if(cleanWord):
		print cleanWord,count
