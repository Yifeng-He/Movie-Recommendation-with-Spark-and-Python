# Movie-Recommendation-with-Spark-and-Python
This project aims to recommend movies to a user based on the movies that have been watched.

# movie_remendation_based_on_similarities.py

This program aims to recommend the movies to a user based on a movie that the user has watched before.

Dataset:

u.DATA: contains (user_id, movie_id, rating, time_stamp)

u.ITEM: contains movie_id and movie_name

To run the program: #spark-submit movie_remendation_based_on_similarities.py



# movie_remendation_using_ALS.py

This program aims to recommend the movies to a user based on the ratings that the user has made.

Dataset:

u.DATA: contains (user_id, movie_id, rating, time_stamp)

u.ITEM: contains movie_id and movie_name

To run the program: # spark-submit movie_remendation_using_ALS.py

# How to Run a Python Program in Amazon EMR Cluster?

1. sign in to Amazon AWS account, and then create an EMR cluster

2. upload the Python script and the input data to your S3 buckets

3. click "add step" to submit a Spark application

4. check the output result in your S3 bucket

An example Python script (word_count_EMR.py) for running in Amazon EMR cluster is provided. 
