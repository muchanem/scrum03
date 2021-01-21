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
def get_prices(stock):
    afilter = {"volume": {"$exists": True}} 
    df = pd.DataFrame.from_records(db[stock].find(afilter))
    df["date"] = df["date"].dt.tz_localize("UTC")
    return df
def send_as_dataframe(dfs):
    for stock, df in dfs.items():
        df.to_dict('records')
        for adict in df:
            db[stock].update_one({'_id': adict['_id']}, {'$set': adict})

def add_weights(dfs):
    for stock, df in dfs.items():
       df["weight"] = (df["followers"])
    return dfs

def convert_times(dfs):
    for stock, df in dfs.items():
        df["date"] = pd.to_datetime(df["date"], format="%a %b %d %H:%M:%S %z %Y")

        dfs[stock] = df
    return dfs
def weighted_average(vals, weights):
    return np.average(vals, weights=weights)
def add_weighted_sentiment(final_dfs, dfs):
    for stock, df in final_dfs.items():
        for index, day in df.iterrows():
            timebefore = day["date"]
            timeafter = timebefore + datetime.timedelta(hours=24)
            ndf = dfs[stock] 
            filtered = ndf.loc[(ndf["date"] >= timebefore) & (ndf["date"] < timeafter)]
            if filtered.empty: 
                df.drop(index=index)
                continue
            df.at[index, "sentiment"] = weighted_average(filtered["compound"], filtered["weight"]) 
    return final_dfs, dfs
def add_weighted_popularity(final_dfs, dfs):
    for stock, df in final_dfs.items():
        for index, day in df.iterrows():
            timebefore = day["date"]
            timeafter = timebefore + datetime.timedelta(hours=24)
            ndf = dfs[stock] 
            filtered = ndf.loc[(ndf["date"] >= timebefore) & (ndf["date"] < timeafter)]
            if filtered.empty: 
                df.drop(index=index)
                continue

            weights = filtered["weight"].sum()
            num_tweets = filtered.shape[0]
            df.at[index,"volume"] = ((weights * num_tweets)/24)
    return final_dfs, dfs
def add_stock_change(final_dfs):
    for stock, df in final_dfs.items():
        ndf = get_prices(stock)
        for index, day in df.iterrows():
            dayrow = ndf.loc[ndf["date"] == day["date"]]
            if dayrow.empty:
                df.drop(index=index)
                continue 
            df.at[index, "delta"] = dayrow.iloc[0]["change"]
    return final_dfs
def get_weighted_data(dfs):
    dfs = add_weights(dfs)
    dfs = convert_times(dfs)
    final_dfs = dict.fromkeys(dfs.keys(),pd.DataFrame(columns=["date", "sentiment", "volume", "delta"]))
    #for stock, df in dfs.items():
    #    df.set_index("date", inplace=True)
    #    dfs[stock] = df

    for stock, df in final_dfs.items():
        idx1 = pd.date_range(start="2014-01-01", end="2016-03-31")
        idx2 =  pd.date_range(start="2019-06-01", end="2019-08-31")
        df["date"] = idx2
        df["date"] = df["date"].dt.tz_localize("UTC")
        final_dfs[stock] = df 
    final_dfs, dfs = add_weighted_sentiment(final_dfs, dfs) 
    final_dfs, dfs = add_weighted_popularity(final_dfs, dfs)
    final_dfs = add_stock_change(final_dfs) 
    return final_dfs

def add_sentiment(final_dfs, dfs):
    for stock, df in final_dfs.items():
        for index, day in df.iterrows():
            timebefore = day["date"]
            timeafter = timebefore + datetime.timedelta(hours=24)
            ndf = dfs[stock] 
            filtered = ndf.loc[(ndf["date"] >= timebefore) & (ndf["date"] < timeafter)]
            if filtered.empty: 
                df.drop(index=index)
                continue
            df.at[index, "sentiment"] = np.average(filtered["compound"])
    return final_dfs, dfs
def add_popularity(final_dfs, dfs):
    for stock, df in final_dfs.items():
        for index, day in df.iterrows():
            timebefore = day["date"]
            timeafter = timebefore + datetime.timedelta(hours=24)
            ndf = dfs[stock] 
            filtered = ndf.loc[(ndf["date"] >= timebefore) & (ndf["date"] < timeafter)]
            if filtered.empty: 
                df.drop(index=index)
                continue

            num_tweets = filtered.shape[0]
            df.at[index,"volume"] = ((num_tweets)/24)
    return final_dfs, dfs
def get_data(dfs):
    dfs = convert_times(dfs)
    final_dfs = dict.fromkeys(dfs.keys(),pd.DataFrame(columns=["date", "sentiment", "volume", "delta"]))
    #for stock, df in dfs.items():
    #    df.set_index("date", inplace=True)
    #    dfs[stock] = df

    for stock, df in final_dfs.items():
        idx1 = pd.date_range(start="2014-01-01", end="2016-03-31")
        idx2 =  pd.date_range(start="2019-06-01", end="2019-08-31")
        df["date"] = idx1.union(idx2)
        df["date"] = df["date"].dt.tz_localize("UTC")
        final_dfs[stock] = df 
    final_dfs, dfs = add_sentiment(final_dfs, dfs) 
    final_dfs, dfs = add_popularity(final_dfs, dfs)
    final_dfs = add_stock_change(final_dfs) 
    return final_dfs




finalthing = get_weighted_data(get_as_dataframe(db.list_collections()))
#finalthing.to_csv("out.csv")

from sklearn import linear_model
for stock, df in finalthing.values():
    df = df.dropna()
    y = df["delta"]
    x = df[["sentiment", "volume"]] 
    linreg = linear_model.LinearRegression()
    linreg.fit(x,y)
    print(stock, end=" ")
    print(linreg.score(x,y))