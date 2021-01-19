#!/usr/bin/env python
# coding: utf-8

# In[863]:


import pymongo
import pandas as pd
from pymongo import MongoClient
from pprint import pprint
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import os


# In[864]:


mongo_uri = "mongodb://myUserAdmin:scrum3ncssm@138.197.6.239:27017"  
client = pymongo.MongoClient(mongo_uri)


# In[865]:


db = client['twitter']
stock= "XOM"


# In[866]:


stock_data = pd.DataFrame(list(db.XOM.find()))


# In[867]:


body = stock_data['body']


# In[868]:


#VADER
analyzer = SentimentIntensityAnalyzer()
stock_data['compound'] = [analyzer.polarity_scores(v)['compound'] for v in stock_data['body']]


# In[869]:


stock_data.head()


# In[870]:


stock_data_dict = stock_data.to_dict('records')


# In[871]:


db[stock].remove({})


# In[872]:


db[stock].insert_many(stock_data_dict)


# In[ ]:





# In[ ]:





# In[ ]:




