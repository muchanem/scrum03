"""
Get prices for all days which we have data
"""

import pymongo, yfinance
from datetime import datetime
class Price(object):
    def __init__(self, date, kind, opening, closing, volume):
        self.date = date 
        self.kind = kind
        self.open = opening
        self.close = closing
        self.change = (closing - opening)
        self.volume = volume

connectionfile = open("connectionstring")
connectionstring = connectionfile.read()
connectionfile.close()
client = pymongo.MongoClient(connectionstring)
db = client["twitter"]
start = "2014-01-01"
end = "2016-03-31"
def givedatetime(x):
    return datetime.strptime(x, '%Y-%m-%d')
for coll in db.collection_names():
    stock = yfinance.Ticker(coll)
    endpoint = db[coll]
    prices = []
    for row in stock.history(start=start, end=end).itertuples():
        prices.append(Price(row.Index.to_pydatetime(), kind="daily", opening=row.Open, closing=row.Close, volume=row.Volume).__dict__)
    if prices: 
        endpoint.insert_many(prices)
