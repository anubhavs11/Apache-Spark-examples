from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperature")
sc = SparkContext(conf = conf)

def parseLine(line):
	fields = line.split(',')
	stationId = fields[0]
	entryType = fields[2]
	temprature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
	return (stationId,entryType,temprature)

lines = sc.textFile("1800.csv")
parsed_lines = lines.map(parseLine)
minTemp = parsed_lines.filter(lambda x:"TMIN" in x[1])
stationTemps = minTemp.map(lambda x:(x[0],x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect();

for result in results:
	print(result[0]+"\t{:.2f}F".format(result[1]))
	
