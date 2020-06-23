'''
Get total amount spent per customer.
Concept used: map, flatMap, key/value RDDs
Input format: customer_ID, item_ID, amount
Input file: Datasets/customer_orders.csv
'''

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('TotalSpendPerCustomer')
sc = SparkContext(conf = conf)

def amountPerCustomer(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile('file:///G:/Courses/Apache_Spark/Datasets/customer_orders.csv')

transaction = input.map(amountPerCustomer)

totalPerCustomer = transaction.reduceByKey(lambda x, y: x + y)

# flip customer_ID and amount then sort on amount
totalPerCustomerSorted= totalPerCustomer.map(lambda x: (x[1], x[0])).sortByKey()

results = totalPerCustomerSorted.collect()

# print sorted output based on amount
for result in results:
    print("{:.2f}".format(result[0]) + ", " +  str(result[1]))


'''
output:

3309.38, 45
3790.57, 79
3924.23, 96
4042.65, 23
4172.29, 99
4178.50, 75
4278.05, 36
4297.26, 98
4316.30, 47
4327.73, 77
4367.62, 13
4384.33, 48
4394.60, 49
4475.57, 94
4505.79, 67
4517.27, 50
4524.51, 78
4561.07, 5
4628.40, 57
4635.80, 83
4642.26, 91
4647.13, 74
4652.94, 84
4659.63, 3
4664.59, 12
4681.92, 66
4701.02, 56
4707.41, 21
4727.86, 80
4735.03, 14
4735.20, 37
4755.07, 7
4756.89, 44
4765.05, 31
4812.49, 82
4815.05, 4
4819.70, 10
4830.55, 88
4836.86, 20
4851.48, 89
4876.84, 95
4898.46, 38
4904.21, 76
4908.81, 86
4915.89, 27
4921.27, 18
4945.30, 53
4958.60, 1
4975.22, 51
4979.06, 16
4990.72, 30
5000.71, 28
5019.45, 22
5032.53, 29
5032.68, 17
5040.71, 60
5057.61, 25
5059.43, 19
5112.71, 81
5123.01, 69
5140.35, 65
5152.29, 11
5155.42, 35
5186.43, 40
5206.40, 87
5245.06, 52
5250.40, 26
5253.32, 62
5254.66, 33
5259.92, 24
5265.75, 93
5288.69, 64
5290.41, 90
5298.09, 55
5322.65, 9
5330.80, 34
5337.44, 72
5368.25, 70
5368.83, 43
5379.28, 92
5397.88, 6
5413.51, 15
5415.15, 63
5437.73, 58
5496.05, 32
5497.48, 61
5503.43, 85
5517.24, 8
5524.95, 0
5637.62, 41
5642.89, 59
5696.84, 42
5963.11, 46
5977.19, 97
5994.59, 2
5995.66, 71
6065.39, 54
6193.11, 39
6206.20, 73
6375.45, 68
'''