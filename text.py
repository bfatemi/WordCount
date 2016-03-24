####################################################
#	Word Count
#	Bobby Fatemi 
#	12/14/15
#
#	Objective: extract wikipedia article and create 
#	count of each word that appears in the article
#
#	Ultimately, the goal is to use spark to distribute
#	the words into partitions, and to map user defined function
#	to split each line into a list of words, and create a 
# 	dictionary where the word is the index and the value is the
#	number of times the word occurs. Initially, this value is 1. 
#	Finally, we use map reduce to count each value by index.

import urllib
from bs4 import BeautifulSoup

#for web scraping use the following function
def getwebtxt(url):
	html = urllib.urlopen(url).read()
	soup = BeautifulSoup(html)

	# kill all script and style elements
	for script in soup(["script", "style"]):
		script.extract()    # rip it out

	# get text
	text = soup.get_text()

	# break into lines and remove leading and trailing space on each
	lines = (line.strip() for line in text.splitlines())
	
	# break multi-headlines into a line each
	chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
	
	# drop blank lines
	text = '\n'.join(chunk for chunk in chunks if chunk)
	return(text)

#user defined functions to split text lines and create dictionary 
def split_words(line):
	return line.split()
	
def create_pair(word):
	return(word,1)
	
def sum_counts(a,b):
	return a + b
	
#load PySpark using the following line (shell):
#PYSPARK_DRIVER_PYTHON=ipython pyspark

pres = getwebtxt("https://en.wikipedia.org/wiki/Barack_Obama")

text_RDD = sc.parallelize(pres.split('\n'))

#how many partitions? count of lines?
text_RDD.getNumPartitions()
text_RDD.count()

#for more functions, check here http://spark.apache.org/docs/latest/programming-guide.html#printing-elements-of-an-rdd
pairs_RDD = text_RDD.flatMap(split_words).map(create_pair)

pairs_RDD.count() #28k words
pairs_RDD.take(10) #see first 10

#sum by index to get count by each word
wordcounts_RDD = pairs_RDD.reduceByKey(sum_counts)
wordcounts_RDD.count() #8k unique words

#use collect to collapse all partitions and see everything, or glom().collect() 
#to see all within partitions
