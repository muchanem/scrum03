import pymongo

connectionfile = open("connectionstring")
connectionstring = connectionfile.read()
connectionfile.close()
client = pymongo.MongoClient(connectionstring)
db = client["twitter"]
count = 0
for coll in db.collection_names():
    print(db[coll].estimated_document_count())
    count += db[coll].estimated_document_count()

print(count)