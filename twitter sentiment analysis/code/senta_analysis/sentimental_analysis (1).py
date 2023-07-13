# -*- coding: utf-8 -*-
"""sentimental_analysis.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/18g6IC3dc6Wq_pKUFi_YMYq6NnMVz4IrW
"""

pip install pyspark

# To implement functions concept
from pyspark.sql import functions as F
# Explode for returning a new row for each and every element in column
from pyspark.sql.functions import explode
# Split for converting delimited separated string into array
from pyspark.sql.functions import split
# For converting twitter text data into StringType and FloatType() for polarity
from pyspark.sql.types import StringType, StructType, StructField, FloatType
# To create spark session
from pyspark.sql import SparkSession
# To implement user defined functions, to select columns,converting json strings into Stringtype()
from pyspark.sql.functions import from_json, col, udf
# For reading the data and matching it with help of regular expressions
import re
# TextBlob for processing textual data
from textblob import TextBlob

spark = SparkSession.builder.appName('Senta').config("mongodb+srv://fw92337:<password>@senta.axjwu4i.mongodb.net/?retryWrites=true&w=majority").getOrCreate()
# Reading json from text file
df_json = spark.read.text("jj.txt")
# To see the dataype of the column
df_json.printSchema()
# To display
df_json.show()

# Defining schema for 'text' column as StringType()
schema = StructType([ StructField("text",StringType(),True)])
# Converting json column to columns
actual_df = df_json.withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.*")
actual_df.printSchema()
actual_df.show()

df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "earliest") \
        .option("kafka.group.id", "group1") \
        .option("subscribe", "twitter") \
        .load()

# sending data to MongoDB
def send_row_in_mongoDB(df):
    mongoDBURL = "mongodb+srv://fw92337:<password>@senta.axjwu4i.mongodb.net/?retryWrites=true&w=majority"
   df.write.format("mongo").mode("append").option("uri", mongoDBURL).save()
  pass

# Only "text" column is required
req_df = actual_df.select("text")
# Defining function to remove unneccesary data
def cleaning_tweet(tweet: str):
# Removing links
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')
# Removing users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
# Removing puntuations
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))
# Removing numbers
    tweet = re.sub('([0-9]+)', '', str(tweet))
# Removing hashtags
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    return tweet
# Calling 'cleaning_tweet' function
clean_tweets = F.udf(cleaning_tweet, StringType())
# Now 'cleaned_text' is added to data with tells that data is not with unncessary stuff
cleaned_tweets = req_df.withColumn('cleaned_text', clean_tweets(col("text")))
# Function to get subjective : perception from subject point of view in the scale 0 to 1
def tweet_subjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity
# Function to get polarity : orienting considered sentiment in the scale -1 to 1
def tweet_polarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity
# Function to get final result :positive, neutral or negative
def tweet_sentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'
# Calling 'tweet_subjectivity' function
get_subjectivity = F.udf(tweet_subjectivity, FloatType())
# Subjectivity is stored in the new column "subject"
subjectivity_tweets = cleaned_tweets.withColumn('subject', get_subjectivity(col("cleaned_text")))
# Calling 'tweet_polarity' function
get_polarity = F.udf(tweet_polarity, FloatType())
# Polarityvalue is stored in the new column "polarityvalue"
polarity_tweets = subjectivity_tweets.withColumn("polarityvalue", get_polarity(col("cleaned_text")))
# Calling 'tweet_sentiment' function
get_sentiment = F.udf(tweet_sentiment, StringType())
# tweet_sentiment is stored in the new column "sentiment"
sentiment_tweets = polarity_tweets.withColumn("sentiment", get_sentiment(col("polarityvalue")))
sentiment_tweets.show()