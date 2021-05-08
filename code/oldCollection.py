# Script to capture the older tweets that were
# initially missed

import tweepy
import csv
import pandas as pd
import pymongo
from pymongo import MongoClient
import json
import time


#input your credentials here
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit_notify=True)

#connect to mongodb
conn = MongoClient('localhost')

#create/name database
db = conn.ps5tweets

#create/name collections Tweets
collection = db.Tweets

#each index is a tweet??
db.tweets.create_index([("id", pymongo.ASCENDING)],unique = True,)

#input keywords here
#keep number of ORs to 6
keyWords = "iphone12 OR iphone5g OR new iphone OR apple iphone OR iphone mini OR iphone max"

def limit_handled(cursor):
    while True:
        try:
            yield cursor.next()
        except tweepy.TweepError:
            t = time.localtime()
            current_time_sleep = time.strftime("%H:%M:%S", t)
            print("Program is now sleeping for 15 minutes. Time is: " +current_time_sleep)
            time.sleep(15 * 60)
            y = time.localtime()
            current_time_wake = time.strftime("%H:%M:%S", y)
            print("Program has Started again. Time is: " + current_time_wake) 
        except StopIteration:
            print("All the tweets have been captured!")
            return

for tweet in limit_handled(tweepy.Cursor(api.search,q=keyWords,
                           lang="en",
                           since="2020-11-13", tweet_mode='extended').items()):
                           
                           collection.insert_one(tweet._json)
        