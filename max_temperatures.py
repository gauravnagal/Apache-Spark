'''
Find out maximum temperatures observed in various weather stations in the year 1800
Concept used: key/value RDDs, filtering RDDs
Input data format: weather_station_ID, date(ymd), obs_type, temp(10 * Degree Celsius)
Input file: Datasets/weather_1800.csv

'''

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    weatherStationID = fields[0]
    obsType = fields[2]
    temperature = float(fields[3]) * 0.1 #convert to Degree Celsius
    return (weatherStationID, obsType, temperature)

# load input data into RDD
lines = sc.textFile("file:///G:/Courses/Apache_Spark/Datasets/weather_1800.csv")

parsedLines = lines.map(parseLine)

# filter takes a function that returns boolean value (True or False), if True - the value is returned to maxTemps RDD
maxTemps = parsedLines.filter(lambda x: 'TMAX' in x[1]) # filters out all bit TMAX

# transform every composite value to new value
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))

# max temperature for every station ID
maxTemp = stationTemps.reduceByKey(lambda x, y: max(x, y))

# print results
results = maxTemp.collect()
for result in results:
    print(result[0] + "\t{:.2f} Degree C".format(result[1]))

'''
Output:
(base) D:\Apache-Spark>spark-submit max_temperatures.py
ITE00100554     32.30Degree C
EZE00100082     32.30Degree C
'''