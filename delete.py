import pymongo
connectionfile = open("connectionstring")
connectionstring = connectionfile.read()
connectionfile.close()
client = pymongo.MongoClient(connectionstring)
db = client["twitter"]
afilter = {"change": {"$exists": True}}

for coll in db.collection_names():
    db[coll].delete_many(afilter)