# For getting twitter api
import tweepy
# To introduce kakfa producer concept
from kafka import KafkaProducer
# For releasing logs 
import logging
# For autheniticating the access keys
from tweepy import OAuthHandler
# For streaming data from Tweepy
from tweepy import Stream
# Getting credentials from stored python file 'securitykeys' 
from securitykeys import consumer_key,consumer_secret,access_token,access_token_secret
# Accesing keys
consumer_key = consumer_key
consumer_secret = consumer_secret
access_token = access_token
access_token_secret = access_token_secret
# Accessing Kafka  server at localhost:9092
producer = KafkaProducer(bootstrap_servers='localhost:9092') 
# our cluster name is "top"
topic_name = "top"
class twitterauth():
# Setting up twitter
    def authenticateTwitterApp(self):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        return auth
class twitterstream():
# Streaming data
    def __init__(self):
        self.twitterAuth = twitterauth()
    def stream_tweets(self):
        while True:
            listener_data = ListenerTS('consumer_key', 'consumer_secret', 'access_token','access_token_secret') 
            auth = self.twitterAuth.authenticateTwitterApp()
            #stream = Stream(auth, listener)
            listener_data.filter(track=["dollar8"], stall_warnings=True, languages= ["en"])
class ListenerTS(tweepy.Stream):
    def on_data(self, raw_data):
            producer.send(topic_name, str.encode(raw_data))
            return True
if __name__ == "__main__":
    TS = twitterstream()
    TS.stream_tweets()