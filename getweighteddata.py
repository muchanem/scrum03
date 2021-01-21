"""
Get tweets from mongodb in a dataframe

"""
import pymongo
import os
import pandas as pd
import datetime
import numpy as np
connectionfile = open("connectionstring")
connectionstring = connectionfile.read()
connectionfile.close()
client = pymongo.MongoClient(connectionstring)
db = client["twitter"]
afilter = {"compound": {"$exists": True}}
def get_as_dataframe(stocks):
    dfs = dict.fromkeys(stocks)
    for stock in stocks:
        dfs[stock] = (pd.DataFrame.from_records(db[stock].find(afilter)))
    return dfs

def send_as_dataframe(dfs):
    for stock, df in dfs.items():
        df.to_dict('records')
        for adict in df:
            db[stock].update_one({'_id': adict['_id']}, {'$set': adict})

def add_weights(dfs):
    for stock, df in dfs.items():
       df["weight"] = ((df["likes"] + df["followers"] * df["retweets"]) + 1)
       #dfs[stock] = df
    return dfs

def convert_times(dfs):
    for stock, df in dfs.items():
        df["date"] = np.vectorize(datetime.datetime.strptime)(df["date"], "%a %b %d %H:%M:%S %z %Y")
        #dfs[stock] = df
    return dfs
def weighted_average(vals, weights):
    return np.average(vals, weights=weights)
def add_weighted_sentiment(final_dfs, dfs):
    for stock, df in final_dfs.items():
        for day in df.itertuples():
            filtered = dfs[stock].loc[day.date:(day.date + datetime.timedelta(hours=24))]
            day.sentiment = weighted_average(filtered["compound"], filtered["weight"]) 
    return final_dfs, dfs
def add_weighted_popularity(final_dfs, dfs):
    for stock, df in final_dfs.items():
        for day in df.itertuples():
            filtered = dfs[stock].loc[day.date:(day.date + datetime.timedelta(hours=24))]
            weights = filtered["weight"].sum()
            num_tweets = filtered.shape[0]
            day.volume = ((weights * num_tweets)/24)
    return final_dfs, dfs
def add_stock_change(final_dfs):
    for stock, df in final_dfs.items():
        for day in df.itertuples():
            afilter = {"date": day.date} 
            day.delta = db[stock].find_one(afilter).change
    return final_dfs
def get_weighted_data(dfs):
    dfs = add_weights(dfs)
    dfs = convert_times(dfs)
    final_dfs = dict.fromkeys(db.collection_names(),pd.DataFrame(columns=["date", "sentiment", "volume", "delta"]))
    for stock, df in dfs.items():
        df.set_index(["date"])
    for stock, df in final_dfs.items():
        df["date"] = (pd.date_range(start="2014-01-01", end="2016-03-31") + pd.date_range(start="2019-06-01", end="2019-08-31"))
        #final_dfs[stock] = df 
    final_dfs, dfs = add_weighted_sentiment(final_dfs, dfs) 
    final_dfs, dfs = add_weighted_popularity(final_dfs, dfs)
    final_dfs = add_stock_change(final_dfs) 
    return final_dfs

def add_sentiment(final_dfs, dfs):
    for stock, df in final_dfs.items():
        for day in df.itertuples():
            filtered = dfs[stock].loc[day.date:(day.date + datetime.timedelta(hours=24))]
            day.sentiment = np.average(filtered["compound"]) 
    return final_dfs, dfs

def add_popularity(final_dfs, dfs):
    for stock, df in final_dfs.items():
        for day in df.itertuples():
            filtered = dfs[stock].loc[day.date:(day.date + datetime.timedelta(hours=24))]
            num_tweets = filtered.shape[0]
            day.volume = (num_tweets/24)
    return final_dfs, dfs


def get_unweighted_data(dfs):
    dfs = convert_times(dfs)
    final_dfs = dict.fromkeys(db.collection_names(),pd.DataFrame(columns=["date", "sentiment", "volume", "delta"]))
    for stock, df in dfs.items():
        df.set_index(["date"])
    for stock, df in final_dfs.items():
        df["date"] = (pd.date_range(start="2014-01-01", end="2016-03-31") + pd.date_range(start="2019-06-01", end="2019-08-31"))
    final_dfs, dfs = add_sentiment(final_dfs, dfs) 
    final_dfs, dfs = add_popularity(final_dfs, dfs)
    final_dfs = add_stock_change(final_dfs) 
    return final_dfs
print(get_weighted_data(get_as_dataframe(["AAPL"])))
#print(get_weighted_data(get_as_dataframe(["AAPL"]))) 