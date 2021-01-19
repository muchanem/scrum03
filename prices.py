"""
Get prices for all days which we have data
"""

import pymongo, yfinance
from datetime import datetime
class Price(object):
    def __init__(self, date, kind, price, volume):
        self.date = date 
        self.kind = kind
        self.price = price
        self.volume = volume

aapl = yfinance.Ticker("AAPL")
print(aapl.history(period="1mo"))
connectionfile = open("connectionstring")
connectionstring = connectionfile.read()
connectionfile.close()
client = pymongo.MongoClient(connectionstring)
db = client["twitter"]
start = "2019-06-01"
end = "2019-08-31"
for coll in db.collection_names():
    stock = yfinance.Ticker(coll)
    endpoint = db[coll]
    prices = []
    for row in stock.history(start=start, end=end).intertuples():
        prices.append(Price(givedatetime(row.Date), kind="open", price=row.Open, volume=row.Volume))
        prices.append(Price(givedatetime(row.Date), kind="close", price=row.Close, volume=row.Volume))
def givedatetime(x):
    return datetime.strptime(x, '%Y-%m-%d')