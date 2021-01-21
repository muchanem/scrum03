#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from sklearn import linear_model


# In[ ]:


df = pd.DataFrame(data.data, columns=data.feature_names)


# In[ ]:


target = pd.DataFrame(data.target, columns=["delta"])


# In[ ]:


X = df
y = target[“delta”]


# In[ ]:


lm = linear_model.LinearRegression()
model = lm.fit(X,y)


# In[ ]:


lm.score(X,y)

