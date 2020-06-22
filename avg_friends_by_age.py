'''
To find average number of friends by age.
Input data: id, name, age, number_of_friends
Input file: Datasets/friendsandage.csv
'''

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("AvgFriendsByAge")
sc = SparkContext(conf = conf)

# Parse (map) the input data
def parseLine(line):
    fields = line.split(',') # split line based on commas
    age = int(fields[2]) # casting string to int value
    numFriends = int(fields[3])
    return (age, numFriends) # return key/value pair

# create lines RDD that's calling text file on the SparkContext with our input data
lines = sc.textFile("file:///Apache_Spark/Datasets/friendsandage.csv")

# transforming lines RDD to rdd RDD. Creates key/value RDD
rdd = lines.map(parseLine)

# calling mapValues on numFriends and transform it, taking resulting RDD and calling reduceByKey on it and adds up all
# values for each unique key, then sorts results by key
totalByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).sortByKey()

# average value by age
avgByAge = totalByAge.mapValues(lambda x: x[0] / x[1])

# print results
results = avgByAge.collect()
for result in results:
    print(result)










