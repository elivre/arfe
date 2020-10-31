#!/usr/bin/env python
# coding: utf-8

# # rede_gephi_com_ipca

# In[ ]:


ano_eleicao = '2014'


# IPCA de outubro de 2014 a outubro de 2018 pela calculadora do Banco Central
# 
# https://www3.bcb.gov.br/CALCIDADAO/publico/corrigirPorIndice.do?method=corrigirPorIndice

# In[ ]:


IPCA_2014_2018 = 1.27872850


# In[3]:


import sys
sys.path.append('../')
import mod_tse as mtse


# In[4]:


import os
home = os.environ["HOME"]
local_dir = f'{home}/temp'


# In[5]:


dbschema = f'rede{ano_eleicao}'

table_gephi_edges = f"{dbschema}.gephi_edges_{ano_eleicao}"
table_gephi_edges_com_ipca = f"{dbschema}.gephi_edges_com_ipca_2018"

table_gephi_nodes = f"{dbschema}.gephi_nodes_{ano_eleicao}"
table_gephi_nodes_com_ipca = f"{dbschema}.gephi_nodes_com_ipca_2018"

table_receitas = f"{dbschema}.receitas_{ano_eleicao}"
table_receitas_com_ipca = f"{dbschema}.receitas_com_ipca_2018"

table_candidaturas = f"{dbschema}.candidaturas_{ano_eleicao}"
table_candidaturas_com_ipca = f"{dbschema}.candidaturas_com_ipca_2018"


# ### ATUALIZA TABELAS PARA REDE COM IPCA

# ### CANDIDATURAS

# In[6]:


query_crate_table_candidaturas_ipca = f"""
    drop table if exists {table_candidaturas_com_ipca} cascade;
    create table {table_candidaturas_com_ipca} as
    select * from {table_candidaturas}
    ;
    
    update {table_candidaturas_com_ipca}
        set receita_total = receita_total * {IPCA_2014_2018},
            despesa_total = despesa_total * {IPCA_2014_2018},
            custo_voto    = custo_voto * {IPCA_2014_2018}
    ;
"""

mtse.execute_query(query_crate_table_candidaturas_ipca)


# ### RECEITAS

# In[7]:


query_crate_table_receitas_ipca = f"""
    drop  table if exists  {table_receitas_com_ipca}; 
    create table  {table_receitas_com_ipca} as
    select * from {table_receitas};
    
    update {table_receitas_com_ipca}
        set receita_valor = receita_valor * {IPCA_2014_2018}
    ;
"""

mtse.execute_query(query_crate_table_receitas_ipca)


# In[ ]:





# In[8]:


def query_update_valores(table):
    colunas_valor = [
        'valor_doado',
        'valor_recebido',
        'fonte_fundo_part',
        'fonte_fundo_esp',
        'fonte_outros_rec',
        'RP',
        'RPF',
        'RPJ',
        'DPI',
        'RPP',
        'RFC',
        'CBRE',
        'RAF',
        'RONI',
        'ROC',
        'DRC',
        'receita_total',
        'despesa_total',
        'custo_voto' 
    ]          
    for cv in colunas_valor:
        mtse.execute_query(f"""
                            update  {table}
                                set {cv} = {cv} * {IPCA_2014_2018};
                           """
                          )


# In[ ]:





# ### NODES

# In[9]:


def query_crate_table_nodes_ipca():
    mtse.execute_query(f"""
    drop  table if exists {table_gephi_nodes_com_ipca} ; 
    create table {table_gephi_nodes_com_ipca} as
    select * from {table_gephi_nodes};
    """)
    
    query_update_valores(table_gephi_nodes_com_ipca)
    

query_crate_table_nodes_ipca()


# ### EDGES

# In[10]:


query_crate_table_edges_ipca  = f"""
    drop  table if exists {table_gephi_edges_com_ipca} ; 
    create table {table_gephi_edges_com_ipca} as
    select * from {table_gephi_edges};
"""

query_crate_table_edges_weight_ipca  = f"""
    update {table_gephi_edges_com_ipca}
        set "Weight" = "Weight" * {IPCA_2014_2018}
    ;
"""

 
mtse.execute_query(query_crate_table_edges_ipca)
mtse.execute_query(query_crate_table_edges_weight_ipca)
query_update_valores(table_gephi_edges_com_ipca)


# In[11]:


import datetime
print(datetime.datetime.now())


# In[ ]:




