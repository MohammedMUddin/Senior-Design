# Script to implement vader sentiment 
# and achieve polarity score for all tweets

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pymongo
from pymongo import MongoClient
import re 
import time
from dateutil.parser import parse


conn = MongoClient('localhost')     # The MongoDB connection info
db = conn.testDB                    # This assumes your database name is testDB
collection = db.finalTweets        # collection to be accessed for tweets
mycol = db["finalVaderTweets"]          # collection to be stored in


analyzer = SentimentIntensityAnalyzer()
tmp = []

# this score is the mean valence score that is
# used to find the compound scores 
# all the words included in here must be lowercase
new_words = {
    "thinner": 2.5,
    "faster": 3.4,
    "sharper": 2.8,
    "cheaper": 2.9,
    "5g": 0.125,
    "longer": 1.8,
    "lidar": 1.9,
    "wireless": 1.9,
    "magsafe": 2.9,
    "expensive": -1.9,
    "costlier": -2.9,
    "slower": -2.7
}

# this updates the SentimentIntensityAnalyzer's Dict with new words.
analyzer.lexicon.update(new_words)


def vaderProcess(sentence):
    vs = analyzer.polarity_scores(sentence)
    vs['sentence'] = sentence
    # print("{:-<65} {}".format(sentence, str(vs)))
    return vs


def insertDocuments():
    t = time.process_time()
    for eachDocument in collection.find({},{ "_id": 0}):
    # for eachDocument in sentences:    
        text = eachDocument['text']
        date = eachDocument['date']
        # rawDate = eachDocument['date']
        # parser = parse(rawDate)
        # date = parser.date()
        newText = vaderProcess(text)     # insert function in here like removeChars
        # mydict = { "text": newText, "date": str(date)}
        mydict = { "date": date, "text": newText}
        x = mycol.insert_one(mydict)
    elapsed_time = time.process_time() - t
    print( "Done! Time Elapsed: " + str(elapsed_time))


insertDocuments()
