import os
import json
import tweepy
from tweepy import OAuthHandler
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import split, explode
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Set your Twitter API credentials
consumer_key = "sTAUcTaC6KPYAXPNmieZSElSy"
consumer_secret = "YF1i8zWoRLrmtRKm2tiUBaCmSqsb5m8MAnVvIaX8Ak7um5Nzfx"
access_token = "122192467-YU7yswLtm380ojKQs9iT6sPg2yaFohYOoPitpsnJ"
access_secret = "cqJRe3DN8dx4fWyqxXEY1hRuBKqhyfnPwgPXjLWCl6tEC"

# Initialize SparkSession and StreamingContext
spark = SparkSession.builder.appName("TwitterStreaming").getOrCreate()
ssc = StreamingContext(spark.sparkContext, batchDuration=5)  # Process data in 5-second batches

# Create a socket stream to receive data from the client
lines = ssc.socketTextStream("127.0.0.1", 9000)

# Function to process tweets in each batch
def process_tweets(rdd):
    if not rdd.isEmpty():
        # Define a schema for the tweets
        schema = ["text"]
        
        # Create a DataFrame from the RDD
        df = rdd.toDF(schema=schema)
        
        # Split the tweets into words
        words = df.select(explode(split(df.text, " ")).alias("tag"))
        
        # Group by hashtags and count occurrences
        hashtag_counts = words.filter(words.tag.startswith("#")).groupBy("tag").count()
        
        # Sort the hashtags by count
        sorted_hashtags = hashtag_counts.orderBy("count", ascending=False)
        
        # Convert the DataFrame to a Pandas DataFrame for visualization
        top_10_df = sorted_hashtags.limit(10).toPandas()
        
        # Clear the previous plot and display the current top 10 hashtags
        plt.clf()
        plt.figure(figsize=(10, 8))
        sns.barplot(x="count", y="tag", data=top_10_df)
        plt.show()

# Process each batch of tweets
lines.foreachRDD(process_tweets)

# Authenticate with Twitter
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

# Start streaming from Twitter
twitter_stream = tweepy.Stream(auth, listener=None)
twitter_stream.filter(track=["petro"], languages=["en"])

# Start the Spark Streaming context
ssc.start()
ssc.awaitTermination()
