#!/usr/bin/env python
# coding: utf-8

# # 00-run-all

# In[ ]:


ano_eleicao = '2014'
dbschema = f'rede{ano_eleicao}'


# In[ ]:


import os
import sys
sys.path.append('../')
import mod_tse as mtse
home = os.environ["HOME"]


# In[ ]:


get_ipython().run_line_magic('run', '005-{dbschema}_dicionarios.ipynb')


# In[ ]:


get_ipython().run_line_magic('run', '010-{dbschema}_candidaturas.ipynb')


# In[ ]:


get_ipython().run_line_magic('run', '020-{dbschema}_converte_receitas.ipynb')


# In[ ]:


get_ipython().run_line_magic('run', '025-{dbschema}_codifica_receitas.ipynb')


# In[ ]:


get_ipython().run_line_magic('run', '030-{dbschema}_codifica_doador_id.ipynb')


# In[ ]:


get_ipython().run_line_magic('run', '035-{dbschema}_orgaos_partidarios.ipynb')


# In[ ]:


get_ipython().run_line_magic('run', '040-{dbschema}_rede_gephi.ipynb')


# In[ ]:


get_ipython().run_line_magic('run', '050-{dbschema}_rede_gephi_com_ipca.ipynb')


# In[ ]:


get_ipython().run_line_magic('run', '055-{dbschema}_rede_gephi_com_ipca_csv.ipynb')


# In[ ]:


get_ipython().run_line_magic('run', '800-{dbschema}_salva_postgres_public_functions.ipynb')


# In[ ]:


import datetime
print(datetime.datetime.now())

