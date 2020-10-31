#!/usr/bin/env python
# coding: utf-8

# ### salva_postgres_public_functions

# In[1]:


ano_eleicao = '2018'


# In[2]:


dbschema = f'rede{ano_eleicao}'


# In[3]:


import sys
sys.path.append('../')
import mod_tse as mtse


# In[4]:


import os
home = os.environ["HOME"]
local_dir = f'{home}/temp'


# In[5]:


text = mtse.dump_functions('public')
f=open(f'999-rede{ano_eleicao}_postgres_public_functions','w')
f.write(text)
f.close()


# In[6]:


import datetime
print(datetime.datetime.now())


# In[ ]:




