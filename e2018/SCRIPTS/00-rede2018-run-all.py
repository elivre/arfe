#!/usr/bin/env python
# coding: utf-8

# # 00-run-all

# In[ ]:


ano_eleicao = '2018'
dbschema = f'rede{ano_eleicao}'


# In[2]:


import os
import sys
sys.path.append('../')
import mod_tse as mtse
home = os.environ["HOME"]


# In[3]:


get_ipython().run_line_magic('run', '005-{dbschema}_dicionarios.ipynb')


# In[4]:


get_ipython().run_line_magic('run', '010-{dbschema}_candidaturas.ipynb')


# In[5]:


get_ipython().run_line_magic('run', '020-{dbschema}_converte_receitas.ipynb')


# In[6]:


get_ipython().run_line_magic('run', '025-{dbschema}_codifica_receitas.ipynb')


# In[7]:


get_ipython().run_line_magic('run', '030-{dbschema}_codifica_doador_id.ipynb')


# In[8]:


get_ipython().run_line_magic('run', '035-{dbschema}_orgaos_partidarios.ipynb')


# In[9]:


get_ipython().run_line_magic('run', '040-{dbschema}_rede_gephi.ipynb')


# In[10]:


get_ipython().run_line_magic('run', '045-{dbschema}_rede_gephi_csv.ipynb')


# In[11]:


get_ipython().run_line_magic('run', '800-{dbschema}_salva_postgres_public_functions.ipynb')


# In[12]:


import datetime
print(datetime.datetime.now())

