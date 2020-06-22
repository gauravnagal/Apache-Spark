'''
To find average number of friends by age
Concept used: key/value RDDs
Input data format: id, name, age, number_of_friends
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
lines = sc.textFile("file:///Apache-Spark/Datasets/friendsandage.csv")

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


'''
Results:
(base) D:\Apache-Spark>spark-submit avg_friends_by_age.py
(18, 343.375)
(19, 213.27272727272728)
(20, 165.0)
(21, 350.875)
(22, 206.42857142857142)
(23, 246.3)
(24, 233.8)
(25, 197.45454545454547)
(26, 242.05882352941177)
(27, 228.125)
(28, 209.1)
(29, 215.91666666666666)
(30, 235.8181818181818)
(31, 267.25)
(32, 207.9090909090909)
(33, 325.3333333333333)
(34, 245.5)
(35, 211.625)
(36, 246.6)
(37, 249.33333333333334)
(38, 193.53333333333333)
(39, 169.28571428571428)
(40, 250.8235294117647)
(41, 268.55555555555554)
(42, 303.5)
(43, 230.57142857142858)
(44, 282.1666666666667)
(45, 309.53846153846155)
(46, 223.69230769230768)
(47, 233.22222222222223)
(48, 281.4)
(49, 184.66666666666666)
(50, 254.6)
(51, 302.14285714285717)
(52, 340.6363636363636)
(53, 222.85714285714286)
(54, 278.0769230769231)
(55, 295.53846153846155)
(56, 306.6666666666667)
(57, 258.8333333333333)
(58, 116.54545454545455)
(59, 220.0)
(60, 202.71428571428572)
(61, 256.22222222222223)
(62, 220.76923076923077)
(63, 384.0)
(64, 281.3333333333333)
(65, 298.2)
(66, 276.44444444444446)
(67, 214.625)
(68, 269.6)
(69, 235.2)
'''
