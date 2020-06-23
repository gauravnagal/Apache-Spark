'''
Get real insights about the nature of a book by studying the words occuring
in it. This program counts the number of words occurance in a text document.
Concept used: map, flatMap, key/value RDDs, regex
Input file: Datasets/pg44671.txt
'''

from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# lower case words
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

'''
Source: http://www.gutenberg.org/cache/epub/44671/pg44671.txt 
The Project Gutenberg EBook of Jack in the Rockies, by George Bird Grinnell (size 414 kB)
Under the terms of the Project Gutenberg License or online at www.gutenberg.org
'''
input = sc.textFile("file:///D:/Apache-Spark/Datasets/pg44671.txt")

# count number of times each word (separated by white space) occurs in the book
words = input.flatMap(normalizeWords)

# countByValue() function
wordsCount = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# flip the (word, counts) pair to (count, words) and sort in reverse order by count
wordsCountSorted = wordsCount.map(lambda x: (x[1], x[0])).sortByKey(ascending = False)

results = wordsCountSorted.collect()

# prints output
for result in results:
    count = result[0]
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(f"{word.decode('ascii')}:" + ' ' + f"{count}")


'''
output:
the: 5306
and: 3694
to: 2346
of: 1825
a: 1716
that: 1163
they: 1016
it: 995
i: 989
he: 981
in: 878
was: 854
you: 727
had: 678
on: 624
as: 561
.
.
.
volunteer: 1
confirmed: 1
necessarily: 1
paper: 1
edition: 1
pg: 1
facility: 1
includes: 1
produce: 1
subscribe: 1
newsletter: 1
'''