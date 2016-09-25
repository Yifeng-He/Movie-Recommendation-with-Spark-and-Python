"""
This program aims to recommend the movies to a user based on a movie that the user has watched before.

Dataset:
u.DATA: contains (user_id, movie_id, rating, time_stamp)
u.ITEM: contains movie_id and movie_name

To run the program:
# spark-submit movie_remendation_based_on_similarities.py
"""

from pyspark import SparkConf, SparkContext
import numpy as np

# create a dictionary for mapping movie id to movie name
def loadMovieNames(filepath):
    movie_name_dict = {}
    with open(filepath) as f:
        for line in f:
            items = line.split('|')
            movie_id = int(items[0])
            movie_name = str(items[1])
            movie_name_dict[movie_id] = movie_name  
    return movie_name_dict

# step-1 processing function
def f_step1(line):
    fields=line.split()
    user_id=int(fields[0])
    movie_id=int(fields[1])
    rating=int(fields[2])
    return (user_id,(movie_id, rating))

# step-3: filtering out the duplicated key-value pairs 
def f_step3(entry):
    (movie_id1, rating1) = entry[1][0]
    (movie_id2, rating2) = entry[1][1]
    return (movie_id1 < movie_id2)

# step-6: calculate the correlation coeffient between a pair of movies
# the input is: [(r11,r21),(r12,r22),(r13, r23)])
def f_step6(entry):
    list_ratingPairs = entry
    movie1_ratings = [x[0] for x in list_ratingPairs]
    movie2_ratings = [x[1] for x in list_ratingPairs]
    # number of pairs in the ratingList
    num_ratings=len(movie1_ratings)
    # correlation coeficient
    corr_coeffient = np.corrcoef(movie1_ratings, movie2_ratings)[0,1]
    return (corr_coeffient, num_ratings)
    
# return the other movie in the movie pair
def getOtherMovie((gievenMovie, entry)):
    movie1=entry[0][0]
    movie2=entry[0][1]
    if gievenMovie == movie1:
        return movie2
    else:
        return movie1
         
# main program
conf=SparkConf().setMaster('local[*]').setAppName('FindSimilarMovie')
sc=SparkContext(conf=conf)

# the file path to map movie id to movie name
filepath = 'u.item'
mapMovieName = loadMovieNames(filepath)

# load the data into RDD
data = sc.textFile('u.data')

# step 1: raw data --> user_id, (movie_id, rating)
RDD1 = data.map(f_step1)

# step 2: self-join --> (user1, ((m1, r1), (m2,r2)))
RDD2 = RDD1.join(RDD1)

# step 3: remove the duplate pairs, f_step3() return a boolean variable
RDD3 = RDD2.filter(f_step3)

# step 4: make pair --> ((m1,m2), (r1,r2))
RDD4 = RDD3.map(lambda (x, y): ((y[0][0], y[1][0]) ,(y[0][1], y[1][1])) )

#step 5: groupByKey (using mapValue() to convert the value to a list)-->((m1,m2), [(r11,r21),(r12,r22),(r13, r23)])
RDD5 = RDD4.groupByKey().mapValues(lambda x: list(x))

# step 6: calculate the similarity for a pair of movies --> ((m1,m2),(coorelation_coefficient, number_of_ratingPairs))
RDD6 = RDD5.mapValues(f_step6).cache() # cache this RDD because it will be used again

# find the similar movies for a given a moviw id
movie_watched=1267

# find the top-k similar movies
k=10
similarMovies = RDD6.filter(lambda x: movie_watched in x[0] and x[1][0] > 0.5 and x[1][1] > 20).sortBy(lambda x: x[1][0], ascending=False).take(k)

# print the result using movie IDs
for item in similarMovies:
    print item
    
# print the result using movie names
print 'Because you watched \" %s \", we recommend you the following similar movies:' % mapMovieName[movie_watched]
for item in similarMovies:
    print '%s with similarity score of %f and rating occurances of %d.' % (mapMovieName[getOtherMovie((movie_watched,item))], item[1][0], item[1][1])

# put the results into numpy array and save it into text
result=np.array([[ x[0][0],x[0][1],x[1][0],x[1][1] ] for x in similarMovies])
np.savetxt('output_find_movies.txt', result)

sc.stop()


