from pyspark.sql import SparkSession,Row
import collections

spark = SparkSession.builder.appName("TempCalc").getOrCreate()

def parseLine(line):
	fields = line.split(',')
	stationId = fields[0]
	entryType = fields[2]
	temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
	return Row(ID=stationId,entryType=entryType,temperature="{:.2f}F".format(temperature))

lines = spark.sparkContext.textFile("1800.csv")
parsed_lines = lines.map(parseLine)

temp = spark.createDataFrame(parsed_lines)
temp.createOrReplaceTempView("temp")

query1 = spark.sql("SELECT * FROM temp WHERE entryType='TMIN'")
query1.createOrReplaceTempView("temp2")
#set result of query1 into a new table temp2

# can be done with same table by combining both queries
query2 = spark.sql("SELECT ID,min(temperature) FROM temp2 GROUP BY ID")

results = query2.collect();

for result in results:
	print(result)

spark.stop()
