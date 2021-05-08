import pymongo
from pymongo import MongoClient
import re 
import time

conn = MongoClient('localhost')     # The MongoDB connection info
db = conn.testDB                    # This assumes your database name is testDB                  
collection = db.dateCleanSmallTweets   # collection to be accessed for tweets
mycol = db.finalSmallTweets          # collection to be stored in
  
# mycol = db["cleanBigDateTweets"]          

def removeChars(sentence):
    retweetPattern = '^RT\s@[\w]*:'
    urlPattern = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    userPattern = '@[\w]*'

    match_retweetPattern = re.findall(retweetPattern, sentence)
    match_URL = re.findall(urlPattern, sentence)
    match_userPattern = re.findall(userPattern, sentence)

    for i in match_retweetPattern:
        sentence = re.sub(i, "", sentence)
    for i in match_URL:
        sentence = re.sub(re.escape(i), "", sentence)
    for i in match_userPattern:
        sentence = re.sub(i, "", sentence)
    newString = sentence.lstrip()

    return newString



def insertDocuments():
    t = time.process_time()
    for eachDocument in collection.find({},{ "_id": 0}):
        fullText = eachDocument["text"]
        date = eachDocument['date']
        newText = removeChars(fullText)     # insert function in here like removeChars
        mydict = { "text": newText, 'date': date}
        x = mycol.insert_one(mydict)
    elapsed_time = time.process_time() - t
    print( "Done! Time Elapsed: " + str(elapsed_time))


insertDocuments()

