#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[2]:


import sys
sys.path.append('../')
import mod_tse as mtse

import os
home = os.environ["HOME"]
local_dir = f'{home}/temp'


import os
import pandas as pd
import numpy as np


get_ipython().run_line_magic('matplotlib', 'inline')
import locale
locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')


pd.options.display.precision = 2
pd.options.display.float_format = '{:,.2f}'.format
pd.options.display.float_format=lambda x: locale.currency(x, grouping=True, symbol=None)


# In[ ]:


select (substring(source,1,16)) from 
rede2018.gephi_edges_2018 ge 
where  doador_tipo = 'CA'

select replace(id,'-RP','') from
rede2018.gephi_nodes_2018 ge 

with DEP as
(
select id from rede2018.gephi_nodes_2018
where id   not like '%-RP' and tipo = 'CA'
and ds_cargo like 'DEPUTADO FEDERAL%'
and ds_sit_tot_turno like 'ELEITO%' 
)


select * from REDE2018.CANDIDATURAs_2018
where 
 ds_cargo like 'DEPUTADO FEDERAL%'
and ds_sit_tot_turno like 'ELEITO%' 
and candidatura_id not in  
(
select id from rede2018.gephi_nodes_2018
where id   not like '%-RP' and tipo = 'CA'
and ds_cargo like 'DEPUTADO FEDERAL%'
and ds_sit_tot_turno like 'ELEITO%' 
)

select target from rede2018.gephi_edges_2018
where target  not like '%-RP' and receptor_tipo = 'CA'
and ds_cargo like 'DEPUTADO FEDERAL%'
and ds_sit_tot_turno like 'ELEITO%' 
group by target

select target from rede2018.gephi_edges_2018
where target  not like '%-RP' 
and target in (
'CAMA5122'
'CAMG4055'
)

select * from rede2018.receitas_2018 r 
where receptor_id in (
'CAMA5122'
'CAMG4055'
)

select * from tse2018.receitas_candidatos_2018 rc  
where nr_candidato = '5122'
and sg_ue = 'MA'

select * from tse2018.receitas_candidatos_2018 rc  
where nr_candidato = '4055'
and sg_ue = 'MG'

select nr_candidato,sit_tot_turno from rede2018.gephi_edges_2018 
where target in 
(
'CAMA5122',
'CAMG4055'
)





select nr_candidato, ds_sit_tot_turno from rede2018.candidaturas_2018 
where candidatura_id in 
(
'CAMA5122',
'CAMG4055'
)


select count(*) from rede2018.candidaturas_2018
where
ds_cargo like 'DEPUTADO FEDERAL%'
and ds_sit_tot_turno like 'ELEITO%' 
and ds_situacao_candidatura ='APTO'

select count(*) from rede2018.gephi_nodes_2018
where
ds_cargo like 'DEPUTADO FEDERAL%'
and ds_sit_tot_turno like 'ELEITO%' 
and ds_situacao_candidatura ='APTO'
and id not like '%-RP'

select count(*) from rede2018.gephi_nodes_2018
where
ds_cargo like 'DEPUTADO FEDERAL%'
and ds_sit_tot_turno like 'ELEITO%' 
and ds_situacao_candidatura <>'APTO'
and id not like '%-RP'

select count(*) from tse2018.consulta_cand_2018 cc 
where
ds_cargo like 'DEPUTADO FEDERAL%'
and ds_sit_tot_turno like 'ELEITO%' 
and ds_situacao_candidatura ='APTO'


select ds_situacao_candidatura, nr_candidato, ds_sit_tot_turno from tse2018.consulta_cand_2018 
where nr_candidato = '5122'
and sg_ue = 'MA'
union 
select ds_situacao_candidatura, nr_candidato, ds_sit_tot_turno from tse2018.consulta_cand_2018 
where nr_candidato = '4050'
and sg_ue = 'MG'

select * from tse2018.consulta_cand_2018 
where nr_candidato = '5122'
and sg_ue = 'MA'

select * from tse2018.consulta_cand_2018 


select 
  doador_id           as Source ,
  receptor_id         as Target,
  'undirected'        as Type,
  sum(receita_valor)  as Weight,
  doador_tipo_cd,
  receptor_tipo_cd,
  receptor_uf
from rede2018.receitas_2018 
where receptor_id in 
(
'CAMA5122',
'CAMG4055'
)
group by 
  doador_id,
  receptor_id,
  doador_tipo_cd,
  receptor_tipo_cd,
  receptor_uf
  
  
  


SELECT
  doador_id           as Source ,
  receptor_id         as Target,
  'undirected'        as Type,
  sum(receita_valor)  as Weight,
  doador_tipo_cd,
  receptor_tipo_cd,
  receptor_uf
FROM rede2018.receitas_2018 r 
where receptor_tipo_cd = 'CA'
group by 
  doador_id,
  receptor_id,
  doador_tipo_cd,
  receptor_tipo_cd,
  receptor_uf


 select * from rede2018.gephi_nodes_2018 gn 
 


# In[45]:


mtse.pandas_query(f"""
select id, nr_candidato, ds_sit_tot_turno, ds_situacao_candidatura from rede2018.gephi_nodes_2018 
where id in 
(
'CAMA5122',
'CAMG4055'
)
""")


# In[46]:


mtse.pandas_query(f"""
select nr_candidato, ds_sit_tot_turno, ds_situacao_candidatura from rede2018.candidaturas_2018 
where candidatura_id in 
(
'CAMA5122',
'CAMG4055'
)
""")


# In[47]:


mtse.pandas_query(f"""
select count(*) from rede2018.gephi_nodes_2018 
where
    ds_cargo like 'DEPUTADO FEDERAL%'
    and ds_sit_tot_turno in ('ELEITO','ELEITO POR MÉDIA','ELEITO POR QP')
    and ds_situacao_candidatura ='APTO'
    and id not like '%-RP'
""")


# In[48]:


mtse.pandas_query(f"""
select count(*) from rede2018.candidaturas_2018 
where
    ds_cargo like 'DEPUTADO FEDERAL%'
    and ds_sit_tot_turno in ('ELEITO','ELEITO POR MÉDIA','ELEITO POR QP')
    and ds_situacao_candidatura ='APTO'
    --and id not like '%-RP'
""")


# In[37]:


mtse.pandas_query(f"""
select candidatura_id, nr_candidato, ds_sit_tot_turno, ds_situacao_candidatura from rede2018.candidaturas_2018 
where
    ds_cargo like 'DEPUTADO FEDERAL%'
    --and ds_sit_tot_turno like 'ELEITO%' 
    --and ds_situacao_candidatura ='APTO'
    and candidatura_id in 
    (
    'CAMA5122',
    'CAMG4055'
    )
""")


# In[41]:


mtse.pandas_query(f"""
select candidatura_id from REDE2018.CANDIDATURAs_2018
where 
ds_cargo like 'DEPUTADO FEDERAL%'
and ds_sit_tot_turno like 'ELEITO%' 
and ds_situacao_candidatura ='APTO'
and candidatura_id not in  
(
select id from rede2018.gephi_nodes_2018
where 
ds_cargo like 'DEPUTADO FEDERAL%'
and ds_sit_tot_turno like 'ELEITO%' 
and ds_situacao_candidatura ='APTO'
and id   not like '%-RP' 
--and tipo = 'CA'
)
"""
                 )


# In[ ]:




