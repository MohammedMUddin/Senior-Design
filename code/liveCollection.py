from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import tweepy
import pymongo
from pymongo import MongoClient
import json

KeyWords = ["iphone12","iphone12mini","appleevent",
                       	"new iphone", "iphone12max", "iphone12pro"]
#consumer key, consumer secret, access token, access secret.
CONSUMER_KEY=""
CONSUMER_SECRET=""
OAUTH_TOKEN=""
OAUTH_TOKEN_SECRET=""



# The MongoDB connection info.
conn = MongoClient('localhost')
# This assumes your database name is IphoneReview.
db = conn.Corona
# Your collection name is tweets.
collection = db.Tweets
db.tweets.create_index([("id", pymongo.ASCENDING)],unique = True,)
class MaxListener(tweepy.StreamListener):
    def on_data(self, raw_data):
        self.process_data(raw_data)
        return True
    def process_data(self, raw_data):
        try:
            tweet = json.loads(raw_data)
            #if collection.count_documents({'id' : tweet.id}) != 0:
             #   pass
            #else:
            collection.insert_one(tweet)
            return True
        except Exception as e:
            print(e)
            pass
    def on_error(self, status_code):
        print(status_code)
        if status_code == 420:
            return False

class MaxStream():
    def __init__(self, auth, listener):
        self.stream = tweepy.Stream(auth=auth, listener=listener, tweet_mode='extended')
    
    def start(self, keyword_list):
        self.stream.filter(track=KeyWords,languages=['en']) 

keyWords = []


if __name__ == "__main__":
    listener = MaxListener()

    authentication = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    authentication.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    tweetStream = MaxStream(authentication, listener)
    # Here write down your keywords which you want to search for.
    tweetStream.start(keyWords)
