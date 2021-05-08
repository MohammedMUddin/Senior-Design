# Script to properly export 1.8 million tweets with only selected fields 
# as exporting directly from Mongo is causing some errors
# remove any fields that are empty
# This method will also result in some empty fields in the MongoDB
# Remove it manually using the MpngoDB Shell

import pymongo
from pymongo import MongoClient
import re 
import time


conn = MongoClient('localhost')     # The MongoDB connection info
db = conn.testDB                    # This assumes your database name is testDB
importedCollection = db.dateSmallTweets        # collection to be accessed for tweets
exportedCollection = db["dateCleanSmallTweets"]          # collection to be stored in


def insertDocuments():
    t = time.process_time()
    for eachDocument in importedCollection.find({},{ "_id": 0}):
        date = eachDocument['created_at']
        if (eachDocument.get('extended_tweet')):
            text = eachDocument['extended_tweet']['full_text']
        elif (eachDocument.get('retweeted_status')):
            text = eachDocument['retweeted_status']['full_text']
        elif (eachDocument.get('retweeted_status') and eachDocument.get('extended_tweet')):
            text = eachDocument['retweeted_status']['full_text']
        else:
            text = eachDocument['full_text']
        mydict = { "date": date, "text": text}
        # print(mydict)
        exportedCollection.insert_one(mydict)
    elapsed_time = time.process_time() - t
    print( "Done! Time Elapsed: " + str(elapsed_time))
 
insertDocuments()
