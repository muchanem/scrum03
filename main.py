"""
When run this code will loop through the companies in the csv files
and get all tweets from a certain start data to end data from a list
of trusted accounts and put them in respective collections on a mongodb
server
"""
import GetOldTweets3 as got
import pymongo
import pandas as pd
accounts=["AP", "reuters"]
startdate="2020-01-01"
enddate="2020-12-30"
client = pymongo.MongoClient('mongodb://138.197.6.239:27017')
db = client["twitter"]
df = pd.read_csv('stocks.csv', header=None)
for i, j in df.iterrows():
    collection = db[j[0]]
    searchstring = j.to_string(na_rep='',index=False).replace(" ", "").replace("\n", " OR ")
    tweetCriteria = got.manager.TweetCriteria().setUsername(accounts)\
                                            .setTopTweets(True)\
                                            .setMaxTweets(10)\
                                            .setSince(startdate)\
                                            .setUntil(enddate)\
                                            .setQuerySearch(searchstring)
    tweet = got.manager.TweetManager.getTweets(tweetCriteria)[0]
    print(tweet.text)
