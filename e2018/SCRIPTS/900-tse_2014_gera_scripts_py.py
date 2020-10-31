#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import sys

import matplotlib.pyplot as plt
from IPython.display import HTML
from tabulate import tabulate
from IPython.display import display
import pandas as pd
import locale
locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')


pd.set_option('precision', 2)
pd.set_option("float_format", locale.currency)
pd.options.display.float_format = '{:20,.2f}'.format

sys.path.append('../')
import mod_tse as mtse
home = os.environ["HOME"]


# ## GERA SCRIPTS DE TODOS OS NOTEBOOKS

# In[2]:


import glob
local_dir = f'SCRIPTS'
if not(os.path.exists(local_dir)):
    os.makedirs(local_dir) 


for file in glob.glob(f"./*.ipynb"):
    print( f"ipython nbconvert --output-dir='SCRIPTS' --to script {file}")
    
    #os.system( f"/home/neilor/anaconda3/bin/jupyter  nbconvert --output-dir='SCRIPTS' --to script {file}")
    os.system( f"/home/neilor/anaconda3/bin/jupyter  nbconvert --output-dir='SCRIPTS' --to script {file}")
    


# In[4]:


import glob
local_dir = f'SCRIPTS'
if not(os.path.exists(local_dir)):
    os.makedirs(local_dir) 

nlinhas = 0
for file in glob.glob(f"./SCRIPTS/*.py"):
    nlinhas+=sum(1 for line in open(file))
print('nlinhas', nlinhas)
#print( file, sum(1 for line in open(file)))


# In[ ]:





# In[5]:


mtse.pandas_query(f"""SELECT table_schema, SUM(row_count) AS total_rows FROM (
  SELECT table_schema, 
         count_rows_of_table(table_schema, table_name) AS row_count
    FROM information_schema.tables
    WHERE table_schema NOT IN ('pg_catalog', 'information_schema') 
      AND table_type='BASE TABLE'
) AS per_table_count_subquery
  GROUP BY table_schema
  ORDER BY 2 DESC;
  """)


# In[6]:


mtse.pandas_query(f"""select
  table_schema,
  table_name,
  count_rows_of_table(table_schema, table_name)
from
  information_schema.tables
where 
  table_schema  in ('pg_catalog', 'information_schema')
  and table_type = 'BASE TABLE'
order by
  1 asc,
  3 desc
  ;
""")


# In[8]:


df_tables=mtse.pandas_query(f"""select
  table_schema,
  table_name,
  count_rows_of_table(table_schema, table_name)
from
  information_schema.tables
where 
  table_schema not in ('pg_catalog', 'information_schema')
  and table_type = 'BASE TABLE'
order by
  1 asc,
  3 desc
  ;
""")


# In[10]:


df_tables.to_excel('tables.xlsx')


# In[ ]:




