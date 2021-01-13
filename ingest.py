"""
Injest data from stocknet dataset
"""
import json
import pymongo
import os
connectionfile = open("connectionstring")
connectionstring = connectionfile.read()
connectionfile.close()
client = pymongo.MongoClient(connectionstring)
db = client["twitter"]

class Tweet(object):
    def __init__(self, date, body, uid, userid, username, followers, rt, likes, verified):
        self.date = date
        self.body = body
        self.uid = uid
        self.userid = userid
        self.username = username
        self.followers = followers
        self.rt = rt
        self.likes = likes
        self.verified = verified
def object_decoder(tweet):
    if 'retweeted_status' in tweet:
        return 'retweet'
    if 'user' in tweet:
        return Tweet(tweet['created_at'], tweet['text'], tweet['id_str'], tweet['user']['id_str'], tweet['user']['screen_name'], tweet['user']['followers_count'], tweet['retweet_count'], tweet['favorite_count'], tweet['user']['verified'])
    return tweet
for stock in os.listdir("stocknet"):
    col = db[stock]
    tweets = []
    for files in os.listdir("stocknet/" + stock):
        file = open(("stocknet/" + stock + "/" + files), "r")
        text = file.readlines()
        file.close()
        for line in text:
            tweet = json.loads(line, object_hook=object_decoder)
            if tweet == 'retweet':
                continue
            tweets.append(tweet.__dict__)
    col.insert_many(tweets) 