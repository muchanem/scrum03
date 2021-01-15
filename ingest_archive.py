"""
Injest data from the internet archive tweet backup
"""
import json
import pymongo
import os
import glob
import tarfile
import bz2
from internetarchive import download
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
    if 'delete' in tweet:
        return 'retweet'
    if 'user' in tweet:
        return Tweet(tweet['created_at'], tweet['text'], tweet['id_str'], tweet['user']['id_str'], tweet['user']['screen_name'], tweet['user']['followers_count'], tweet['retweet_count'], tweet['favorite_count'], tweet['user']['verified'])
    return tweet
stocksofinterest = ["$" + x for x in os.listdir("stocknet")]
def checkfor(body):
    for stock in stocksofinterest:
        if stock in body:
            return [True, stock[1:]]
    return [False, ""]
cols = dict.fromkeys(os.listdir("stocknet"), [])

archivedir = "archiveteam-twitter-stream-2019-06"
download(archivedir, verbose=True)
for file in os.listdir(archivedir):
    if file.endswith(".tar"):
        tar = tarfile.open((archivedir +"/" + file))
        tar.extractall(path="working")
        tar.close()
for file in glob.glob("working" + "/**/*.json.bz2", recursive=True):
    with bz2.open(file, "rt") as bz_file:
        text = bz_file.readlines()
        for line in text:
            tweet = json.loads(line, object_hook=object_decoder)
            if tweet == 'retweet':
                continue
            returned = checkfor(tweet.body)
            if returned[0]:
                cols[returned[1]].append(tweet.__dict__)

for col in cols:
    acol = db[col]
    print(col + ": " + str(len(cols[col])))
    acol.insert_many(cols[col])