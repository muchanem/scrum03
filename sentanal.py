import pymongo
import os
import pandas as pd
import numpy as np
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

connectionfile = open("connectionstring")
connectionstring = connectionfile.read()
connectionfile.close()
client = pymongo.MongoClient(connectionstring)
db = client["twitter"]
afilter = {"compound": {"$exists": False}, "body": {"$exists": True}}
def get_as_dataframe(stocks):
    dfs = dict.fromkeys(stocks)
    for stock in stocks:
        df = (pd.DataFrame.from_records(db[stock].find(afilter)))
        #ldf = df.loc[(df["compound"] >= 0.025)]
        #gdf = df.loc[df["compound"] <= -0.025] 
        #df = pd.concat([ldf, gdf])
        dfs[stock] = df
    return dfs

analyzer = SentimentIntensityAnalyzer()
def sentimentanal(x):

    return analyzer.polarity_scores(x)["compound"]

def send_as_dataframe(dfs):
    for stock, df in dfs.items():
        df = df.to_dict('records')
        for adict in df:
            db[stock].update_one({'_id': adict['_id']}, {'$set': adict})

dfs = get_as_dataframe(db.collection_names())
for stock, df in dfs.items(): 
    for index, tweet in df.iterrows():
        df.at[index,"compound"] = sentimentanal(tweet["body"]) 

send_as_dataframe(dfs)