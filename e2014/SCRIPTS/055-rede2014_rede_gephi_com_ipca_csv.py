#!/usr/bin/env python
# coding: utf-8

# # rede_gephi_com_ipca_csv

# In[6]:


ano_eleicao = '2014'

rede =f'rede{ano_eleicao}'

csv_dir = f'/home/neilor/{rede}'


# In[7]:


dbschema = f'rede{ano_eleicao}'
table_edges = f"{dbschema}.gephi_edges_com_ipca_2018"
table_nodes = f"{dbschema}.gephi_nodes_com_ipca_2018"
table_receitas = f"{dbschema}.receitas_com_ipca_2018"
table_candidaturas = f"{dbschema}.candidaturas_com_ipca_2018"

table_municipios = f"{dbschema}.municipios_{ano_eleicao}"


# In[8]:


import sys
sys.path.append('../')
import mod_tse as mtse


# In[9]:


import os
home = os.environ["HOME"]
local_dir = f'{home}/temp'


# In[10]:


mtse.execute_query(f"update {table_municipios} set rede= 'N';")


# ## REDE BRASIL

# In[11]:


def salva_rede_brasil(csv_dir,rede):   
    rede_dir_BR = f'{csv_dir}/{rede}_Brasil'
    os.makedirs(rede_dir_BR)  

    edges_csv_query=f"""copy
                    (
                    select * from {table_edges}
                    )
                    TO '{rede_dir_BR}/{rede}_Brasil_edges.csv' DELIMITER ';' CSV HEADER;
                 """       
    mtse.execute_query(edges_csv_query)

    nodes_csv_query=f"""copy
                    (
                    select * from {table_nodes}
                    )
                    TO '{rede_dir_BR}/{rede}_Brasil_nodes.csv' DELIMITER ';' CSV HEADER;
                 """       
    mtse.execute_query(nodes_csv_query)

    candidaturas_csv_query=f"""copy
                    (
                    select * from {table_candidaturas}
                    )
                    TO '{rede_dir_BR}/{rede}_Brasil_candidaturas.csv' DELIMITER ';' CSV HEADER;
                 """       
    mtse.execute_query(candidaturas_csv_query)

    receitas_csv_query=f"""copy
                    (
                    select * from {table_receitas}
                    )
                    TO '{rede_dir_BR}/{rede}_Brasil_receitas.csv' DELIMITER ';' CSV HEADER;
                 """    
    mtse.execute_query(receitas_csv_query)
    


# ## REDES POR ESTADO

# In[12]:


def salva_rede_csv_uf(csv_dir,rede,sg_uf):   
    rede_dir_uf = f'{csv_dir}/{rede}_{sg_uf}'
    os.makedirs(rede_dir_uf)  
    
    edges_query=f"""copy
                    (
                    select * from {table_edges} where  ue ='{sg_uf}'
                    )
                    TO '{rede_dir_uf}/{rede}_{sg_uf}_edges.csv' DELIMITER ';' CSV HEADER;
                 """       
    mtse.execute_query(edges_query)

    nodes_query=f"""copy
                    (
                    select * from {table_nodes} where  ue ='{sg_uf}'
                    )
                    TO '{rede_dir_uf}/{rede}_{sg_uf}_nodes.csv' DELIMITER ';' CSV HEADER;
                 """

    mtse.execute_query(nodes_query)
    
    candidaturas_csv_query=f"""copy
                    (
                    select * from {table_candidaturas} where  sg_uf ='{sg_uf}'
                    )
                    TO '{rede_dir_uf}/{rede}_{sg_uf}_candidaturas.csv' DELIMITER ';' CSV HEADER;
                 """       
    mtse.execute_query(candidaturas_csv_query)

    receitas_csv_query=f"""copy
                    (
                    select * from {table_receitas} where  receptor_uf ='{sg_uf}'
                    )
                    TO '{rede_dir_uf}/{rede}_{sg_uf}_receitas.csv' DELIMITER ';' CSV HEADER;
                 """       
    mtse.execute_query(receitas_csv_query)
    


# In[13]:


import pandas as pd
import shutil

if os.path.exists(csv_dir):
    shutil.rmtree(csv_dir)
os.makedirs(csv_dir)  
 
salva_rede_brasil(csv_dir,rede)
    
df_uf = mtse.pandas_query(f'select sg_uf from {table_candidaturas} group by sg_uf order by sg_uf')                         
for index, row in df_uf.iterrows():
    sg_uf = row['sg_uf']
    salva_rede_csv_uf(csv_dir,rede,sg_uf)


# In[14]:


import datetime
print(datetime.datetime.now())


# In[ ]:




