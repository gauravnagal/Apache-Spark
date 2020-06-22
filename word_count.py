'''
Count the number of words occurance in a text document
Concept used: map, flatMap, key/value RDDs
Input file: Datasets/pg44671.txt
'''

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

'''
Source: http://www.gutenberg.org/cache/epub/44671/pg44671.txt 
The Project Gutenberg EBook of Jack in the Rockies, by George Bird Grinnell (size 414 kB)
Under the terms of the Project Gutenberg License or online at www.gutenberg.org
'''
input = sc.textFile("file:///G:/Courses/Apache_Spark/Datasets/pg44671.txt")

# count number of times each word (separated by white space) occurs in the book
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

# prints output
for word, count in wordCounts.items():
    word = word.encode('ascii', 'ignore')
    if word:
        print(f"{word.decode('ascii')}:" + ' ' + f"{count}")