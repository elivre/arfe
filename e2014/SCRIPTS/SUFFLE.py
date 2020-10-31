#!/usr/bin/env python
# coding: utf-8

# ### SHUFFLE UMA SEQUENCIA DE NÚMEROS

# In[1]:


import random


# In[2]:


x=[y+1 for y in range(21)]


# In[3]:


def myfunction():
  return 0.4


# In[4]:


random.shuffle(x,myfunction)
for n in x:
    print(n)


# In[ ]:




