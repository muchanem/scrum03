#!/usr/bin/env python
# coding: utf-8

import pymongo
import pandas as pd
from pymongo import MongoClient
from pprint import pprint
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import os

mongo_uri = "mongodb://myUserAdmin:scrum3ncssm@138.197.6.239:27017"  
client = pymongo.MongoClient(mongo_uri)


db = client['twitter']
stock= "XOM"


stock_data = pd.DataFrame(list(db.XOM.find()))


body = stock_data['body']


#VADER
analyzer = SentimentIntensityAnalyzer()
stock_data['compound'] = [analyzer.polarity_scores(v)['compound'] for v in stock_data['body']]


stock_data.head()


stock_data_dict = stock_data.to_dict('records')


db[stock].remove({})


db[stock].insert_many(stock_data_dict)
