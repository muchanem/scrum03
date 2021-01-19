#!/usr/bin/env python
# coding: utf-8

import pymongo
import pandas as pd
from pymongo import MongoClient
from pprint import pprint
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import os

#Add client in "MongoDB_CLient"
mongo_uri = "MongoDB_Client"  
client = pymongo.MongoClient(mongo_uri)


db = client['twitter']
#Replace "Stock" with desired stock for sentanalysis
stock= "Stock"

#Replace "Stock" with desired stock for sentanalysis
stock_data = pd.DataFrame(list(db.Stock.find()))


body = stock_data['body']


#VADER
analyzer = SentimentIntensityAnalyzer()
stock_data['compound'] = [analyzer.polarity_scores(v)['compound'] for v in stock_data['body']]


stock_data.head()


stock_data_dict = stock_data.to_dict('records')


db[stock].remove({})


db[stock].insert_many(stock_data_dict)
