"""
Get tweets from mongodb in a dataframe

"""
import pymongo
import os
import pandas as pd
connectionfile = open("connectionstring")
connectionstring = connectionfile.read()
connectionfile.close()
client = pymongo.MongoClient(connectionstring)
db = client["twitter"]
def get_as_dataframe(stocks):
    dfs = dict.fromkeys(stocks)
    for stock in stocks:
        dfs[stock] = (pd.DataFrame.from_records(db[stock].find()))
    return dfs

def send_as_dataframe(dfs):
    for stock, df in dfs:
        df.to_dict('records')
        for adict in df:
            db[stock].update_one({'_id': adict['_id']}, {'$set': adict})