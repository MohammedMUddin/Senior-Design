# Script to merge two collections into one.

import pymongo
from pymongo import MongoClient
import time

conn = MongoClient('localhost')     # The MongoDB connection info
db = conn.testDB                    # This assumes your database name is testDB                  
importedCollection = db.dateCleanTweets   # collection to be accessed for tweets
exportedCollection = db.finalTweets          # collection to be stored in

def insertDocuments():
    t = time.process_time()
    for eachDocument in importedCollection.find({},{ "_id": 0}):
        date = eachDocument['date']
        text = eachDocument['text']
        # if (eachDocument.get('extended_tweet')):
        #     text = eachDocument['extended_tweet']['full_text']
        # elif (eachDocument.get('retweeted_status')):
        #     text = eachDocument['retweeted_status']['full_text']
        # elif (eachDocument.get('retweeted_status') and eachDocument.get('extended_tweet')):
        #     text = eachDocument['retweeted_status']['full_text']
        # else:
            
        mydict = { "date": date, "text": text}
        # print(mydict)
        exportedCollection.insert_one(mydict)
    elapsed_time = time.process_time() - t
    print( "Done! Time Elapsed: " + str(elapsed_time))
 
insertDocuments()

