from pyspark.sql import SparkSession,Row
import collections
import re
#importing re to use regular expression

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def normalizeWords(text):
	words = re.compile(r'\W+').split(text.lower())
	return (word=words)

lines = spark.sparkContext.textFile("book.txt")
words = lines.map(normalizeWords)

normalizeWords = spark.createDataFrame(words)
normalizeWords.createOrReplaceTempView("words")

query1 = spark.sql("SELECT count(word) FROM words ORDER BY count(ascending = False)")

results = query1.collect()

for result in results:
	print(result)
