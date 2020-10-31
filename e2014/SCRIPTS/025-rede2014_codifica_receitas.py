#!/usr/bin/env python
# coding: utf-8

# # 025-rede_codifica_receitas

# In[1]:


ano_eleicao = '2014'


# In[2]:


dbschema = f'rede{ano_eleicao}'
table_receitas = f'{dbschema}.receitas_{ano_eleicao}'
table_origem_receitas = f'{dbschema}.origem_receitas_{ano_eleicao}'
table_fonte_receitas = f'{dbschema}.fonte_receitas_{ano_eleicao}'
table_municipios = f"{dbschema}.municipios_{ano_eleicao}"
table_partidos = f'{dbschema}.partidos_{ano_eleicao}'


# In[3]:


import os
import sys
sys.path.append('../')
import mod_tse as mtse
home = os.environ["HOME"]


# In[4]:


mtse.execute_query(f'CREATE SCHEMA IF NOT EXISTS {dbschema};')


# # ATENÇÂO 

# In[2]:


atualizar receptor_cargo_ds (vice s suplentee 2014?????


# ## CODIFICA DESCRIÇÃO, CÓDIGO E SIGLA DA ORIGEM DA RECEITA

# In[5]:


query_update_origem_receita = f"""
update {table_receitas} as r
    set receita_origem_sg = o.sg_origem_receita,
        receita_origem_cd = o.cd_origem_receita,
        receita_origem_ds = o.ds_origem_receita
from {table_origem_receitas} as o
where 
(
    r.receita_origem_cd not in ('#NULO#','#NULO','#NE','') 
    and
    r.receita_origem_sg in ('#NULO#','#NULO','#NE','') 
    and
    upper(r.receita_origem_cd) = upper(o.cd_origem_receita)
)
or 
(    
    r.receita_origem_ds    not in ('#NULO#','#NULO','#NE','') 
    and
    upper(r.receita_origem_cd) in ('#NULO#','#NULO','#NE','') 
    and
    upper(r.receita_origem_ds) = upper(o.tx_origem_receita)
)
;
"""

mtse.execute_query(query_update_origem_receita)


# ## CODIFICA DESCRIÇÃO E CÓDIGO DA FONTE DA RECEITA

# In[6]:


query_update_fonte_receita = f"""
update {table_receitas} as r
    set receita_fonte_cd = f.cd_fonte_receita,
        receita_fonte_ds = f.ds_fonte_receita
from {table_fonte_receitas} as f
where 
(
    r.receita_fonte_cd  not in ('#NULO#','#NULO','#NE','') 
    and
    r.receita_fonte_ds  in ('#NULO#','#NULO','#NE','')
    and
    upper(r.receita_fonte_cd) = upper(f.cd_fonte_receita)
)
or
(
    receita_fonte_ds not in ('#NULO#','#NULO','#NE','') 
    and
    receita_fonte_cd in ('#NULO#','#NULO','#NE','') 
    and
    upper(r.receita_fonte_ds) = upper(f.tx_fonte_receita)
)
;
"""

mtse.execute_query(query_update_fonte_receita)


# ## CODIFICA UF E MUNICÍPIO DO DOADOR 

# In[7]:


siglas_ue = mtse.get_federacao_siglas_ue()

query_update_uf_ue_doador = f"""
update {table_receitas} as r
    set doador_uf      = m.sg_uf,
        doador_ue_nome = m.nm_municipio,
        doador_ue      = m.cd_municipio
from {table_municipios} as m
where 
    (
        doador_ue not in ('#NULO#','#NULO','#NE','') 
        and 
        doador_ue = m.cd_municipio 
        and 
        doador_ue_nome in ('#NULO#','#NULO','#NE','') 
    )
    or
    (
        doador_ue_nome not  in ('#NULO#','#NULO','#NE','')  
        and 
        upper(doador_ue_nome) = upper(m.nm_municipio) 
        and 
        doador_ue in ('#NULO#','#NULO','#NE','') 
    )
;

update {table_receitas} as r
    set doador_uf      = doador_ue,
        doador_ue_nome = doador_ue
where
    doador_uf in ('#NULO#','#NULO','#NE','','-1')
    and 
    doador_ue in ({siglas_ue})
;   

"""

mtse.execute_query(query_update_uf_ue_doador)


# In[ ]:





# ## CODIFICA UF E MUNICÍPIO DO RECEPTOR

# In[8]:


siglas_ue = mtse.get_federacao_siglas_ue()

query_update_uf_ue_receptor = f"""
update {table_receitas} as r
    set receptor_uf      = m.sg_uf,
        receptor_ue_nome = m.nm_municipio,
        receptor_ue      = m.cd_municipio
from {table_municipios} as m
where 
    (
        receptor_ue not in  ('#NULO#','#NULO','#NE','') 
        and 
        receptor_ue = m.cd_municipio 
        and 
        receptor_ue_nome in ('#NULO#','#NULO','#NE','')  
    )
    or
    (
        receptor_ue_nome not  in ('#NULO#','#NULO','#NE','')  
        and 
        upper(receptor_ue_nome) = upper(m.nm_municipio) 
        and 
        receptor_ue in  ('#NULO#','#NULO','#NE','') 
    )
;

update {table_receitas} as r
    set receptor_uf = receptor_ue,
        receptor_ue_nome = receptor_ue
where
    receptor_uf in ('#NULO#','#NULO','#NE','','-1')
    and 
    receptor_ue in ({siglas_ue})
;   

"""

mtse.execute_query(query_update_uf_ue_receptor)


# ## CODIFICA SIGLA/NUMERO DO PARTIDO DO DOADOR 

# In[9]:


query_update_doador_partido_sg = f"""

update {table_receitas} 
    set 
        doador_partido_sg   = sg_partido,
        doador_partido_nr   = nr_partido        
from {table_partidos} 
where 
    (
        doador_partido_sg not in ('#NULO#','#NULO','#NE','') 
        and
        upper(doador_partido_sg) = upper(sg_partido)
        and
        doador_partido_nr in  ('#NULO#','#NULO','#NE','') 
    )
    or
    (
        doador_partido_nr not in ('#NULO#','#NULO','#NE','')  
        and
        upper(doador_partido_nr) = upper(nr_partido)
        and
        doador_partido_sg in  ('#NULO#','#NULO','#NE','')  
    )
    or
    (
        doador_nome_rfb not in ('#NULO#','#NULO','#NE','') 
        and
        receita_origem_sg ='RPP'
        and 
        (
            upper(doador_nome_rfb) like '%'||upper(nm_partido)||'%' 
            or
            upper(doador_nome_rfb) like '%'||upper(sg_partido)||'%' 
        ) 
    )
;
"""

mtse.execute_query(query_update_doador_partido_sg)


# ## CODIFICA SIGLA/NUMERO DO PARTIDO DO RECEPTOR

# In[10]:


query_update_receptor_partido_sg = f"""
update {table_receitas} 
    set 
        receptor_partido_sg   = sg_partido,
        receptor_partido_nr   = nr_partido       
from {table_partidos} 
where 
    (
        receptor_partido_sg not in ('#NULO#','#NULO','#NE','') 
        and
        upper(receptor_partido_sg) = upper(sg_partido)
        and
        receptor_partido_nr in  ('#NULO#','#NULO','#NE','') 
    )
    or
    (
        receptor_partido_nr not in ('#NULO#','#NULO','#NE','')  
        and
        upper(receptor_partido_nr) = upper(nr_partido)
        and
        receptor_partido_sg in  ('#NULO#','#NULO','#NE','')  
    )
;
"""

mtse.execute_query(query_update_receptor_partido_sg)


# In[ ]:





# In[11]:


table_esferas_partidarias = f'{dbschema}.esferas_partidarias_{ano_eleicao}'
query_update_esfera_partidaria = f"""
update {table_receitas} as r
    set receptor_esfera_partidaria_cd = e.cd,
        receptor_esfera_partidaria_ds = e.ds
from {table_esferas_partidarias} as e
where 
    upper(r.receptor_nome) = upper(e.tipo)
    or
    upper(r.receptor_nome) = upper(e.ds)    
;

update {table_receitas} as r
    set doador_esfera_partidaria_cd = e.cd,
        doador_esfera_partidaria_ds = e.ds
from {table_esferas_partidarias} as e
where 
    upper(r.doador_nome) = upper(e.tipo)
    or
    upper(r.doador_nome) = upper(e.ds)    
;
"""

mtse.execute_query(query_update_esfera_partidaria)


# In[12]:


copia_receitas = f"""
DROP TABLE IF EXISTS  {table_receitas}_codificada CASCADE;
create table {table_receitas}_codificada as
select *  from {table_receitas}
;
"""
mtse.execute_query(copia_receitas)


# In[13]:


import datetime
print(datetime.datetime.now())


# In[ ]:




