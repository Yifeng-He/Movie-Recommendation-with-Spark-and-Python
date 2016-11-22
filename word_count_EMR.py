# -*- coding: utf-8 -*-
"""
Created on Fri Sep 16 16:18:21 2016

@author: yifeng
"""

from pyspark import SparkConf, SparkContext
# regular expression
import re

conf=SparkConf()
sc=SparkContext(conf=conf)

lines=sc.textFile("s3://yifengsparkdata/Book.txt")

# processing pipeline
# using regular expression '\W+' to extract only words
word_counts_RDD=lines.flatMap(lambda x: re.split('\W+', x.lower())).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).map(lambda (x,y):(y,x)).sortByKey()

# the list
word_counts = word_counts_RDD.collect()

# debug
#print word_counts.collect()[0]

#print the result
for word in word_counts:
    print '%s: %d' % (word[1], word[0])

# save the results into text file
word_counts_RDD.saveAsTextFile("s3://yifengsparkoutput/word_count_output")

sc.stop()
