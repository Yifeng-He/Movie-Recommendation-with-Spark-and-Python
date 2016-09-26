"""
This program aims to recommend the movies to a user based on the ratings that the user has made.

Dataset:
u.DATA: contains (user_id, movie_id, rating, time_stamp)
u.ITEM: contains movie_id and movie_name

To run the program:
# spark-submit movie_remendation_using_ALS.py
"""

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, Rating

# create dictionary for mapping movie id to movie name
def createMovieDict(filepath):
    dict1 = {}
    with open(filepath) as f:
        for line in f:
            fields = line.split('|')
            movie_id = int(fields[0])
            movie_name = str(fields[1])
            dict1[movie_id] = movie_name
    return dict1

# conver RDD to Rating
def getRating(line):
    fields = line.split()
    user_id = int(fields[0])
    movie_id = int(fields[1])
    score = float(fields[2])
    rating = Rating(user_id, movie_id, score)
    return rating

# set up sparkcontext
conf = SparkConf().setMaster('local').setAppName('MovieRecommendation')
sc = SparkContext(conf=conf)

# load data
movie_dict = createMovieDict('u.item')
sc.broadcast(movie_dict)
data = sc.textFile('u.data')

# convert to Rating object, Represents a (user, product, rating) tuple
ratings = data.map(getRating)
print ratings.collect()[0]

# the ALS model
rank = 10
numIterations = 6
model = ALS.train(ratings, rank, numIterations)

# make prediction
userID = 10
# the rating s that the user has done
print "User %d has made the folowing ratings:" % userID
userRatings = ratings.filter(lambda x: x[0] == userID)
for item in userRatings.collect():
    print movie_dict[item[1]], ': %.1f' % item[2]

# top 10 recommendations
k = 10
#returns a list of Rating objects sorted by the predicted rating in descending order
recommendations=model.recommendProducts(userID, k)
print '\nThe recommended movies are:'
for item in recommendations:
    print movie_dict[item[1]],'with recommended score: %.2f' % item[2]
    
sc.stop()

