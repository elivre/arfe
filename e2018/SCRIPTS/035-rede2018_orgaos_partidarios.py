#!/usr/bin/env python
# coding: utf-8

# ## ORGÃOS PARTIDÁRIOS 2018

# In[ ]:


ano_eleicao = '2018'
dbschema = f'rede{ano_eleicao}'
table_orgaos_partidarios =  f"{dbschema}.orgaos_partidarios_{ano_eleicao}"
table_receitas_orgaos_partidarios = f'tse{ano_eleicao}.receitas_orgaos_partidarios_{ano_eleicao}'
table_esferas_partidarias = f"{dbschema}.esferas_partidarias_{ano_eleicao}"
table_receitas = f"{dbschema}.receitas_{ano_eleicao}"
table_candidaturas = f"{dbschema}.candidaturas_{ano_eleicao}"


# In[2]:


import os
import sys
sys.path.append('../')
import mod_tse as mtse
home = os.environ["HOME"]


# In[3]:


local_dir = f'{home}/temp'


# In[4]:


query_create_table_orgaos_partidarios = f"""

drop table if exists {table_orgaos_partidarios} cascade;

CREATE TABLE {table_orgaos_partidarios} (
    orgao_partidario_id varchar,
    cnpj                varchar,
    nome                varchar,
    uf                  varchar,
    ue                  varchar,
    ue_nome             varchar,
    partido_sg          varchar,
    partido_nr          varchar,
    esfera_partidaria_cd   varchar,
    esfera_partidaria_ds   varchar,
    esfera_partidaria_sg   varchar,
    label               varchar    
);

CREATE INDEX ON {table_orgaos_partidarios} (orgao_partidario_id);

"""

mtse.execute_query(query_create_table_orgaos_partidarios)


# In[ ]:





# In[5]:


query_insert_orgaos_partidarios_receptores = f"""

insert into {table_orgaos_partidarios}
(
		select 
			receptor_id as orgao_partidario_id ,
			receptor_cnpj  as cnpj,
			receptor_nome  as nome,
			receptor_uf  as uf,
            receptor_ue  as ue,
            receptor_ue_nome as ue_nome,
			receptor_partido_sg as partido_sg,
			receptor_partido_nr as partido_nr,
			receptor_esfera_partidaria_cd  as esfera_partidaria_cd,
			receptor_esfera_partidaria_ds  as esfera_partidaria_ds,
            ''                             as esfera_partidaria_sg            
			from {table_receitas}
			where
			receptor_tipo_cd = 'OP'
			group by 
			receptor_id ,
			receptor_cnpj ,
			receptor_nome ,
			receptor_uf ,
            receptor_ue,
            receptor_ue_nome,
			receptor_partido_sg ,
			receptor_partido_nr ,
			receptor_esfera_partidaria_cd  ,
			receptor_esfera_partidaria_ds
)
;
"""



query_insert_orgaos_partidarios_doadores = f"""

insert into {table_orgaos_partidarios}
(
select 		    doador_id ,
				doador_cpf_cnpj ,
				doador_nome_rfb ,
				doador_uf ,
	            doador_ue,
	            doador_ue_nome,
				doador_partido_sg ,
				doador_partido_nr ,
				doador_esfera_partidaria_cd  ,
				doador_esfera_partidaria_ds
	from {table_receitas} as r
	where
	doador_tipo_cd = 'OP'
	and doador_id not in (select orgao_partidario_id from {table_orgaos_partidarios} op )
	and r.receita_origem_sg = 'RPP'
    and doador_cpf_cnpj not in (select nr_cnpj_prestador_conta from {table_candidaturas} )
	group by 	
                doador_id ,
				doador_cpf_cnpj ,
				doador_nome_rfb ,
				doador_uf ,
	            doador_ue,
	            doador_ue_nome,
				doador_partido_sg ,
				doador_partido_nr ,
				doador_esfera_partidaria_cd  ,
				doador_esfera_partidaria_ds
);
"""


mtse.execute_query(query_insert_orgaos_partidarios_receptores)
mtse.execute_query(query_insert_orgaos_partidarios_doadores)


# In[6]:


mtse.pandas_query(f"""
select orgao_partidario_id from (
select orgao_partidario_id,count(*) qtd from {table_orgaos_partidarios} op 
group by orgao_partidario_id
) t
where qtd >1
;
""")


# In[7]:


mtse.execute_query(f"""
delete from {table_orgaos_partidarios}
where orgao_partidario_id in (
select orgao_partidario_id from (
select orgao_partidario_id,count(*) qtd from {table_orgaos_partidarios} op 
group by orgao_partidario_id
) t
where qtd >1
)
and esfera_partidaria_cd = 'M'
;
"""
                  )


# In[8]:


mtse.pandas_query(f"""
select orgao_partidario_id from (
select orgao_partidario_id,count(*) qtd from {table_orgaos_partidarios} op 
group by orgao_partidario_id
) t
where qtd >1
;
""")


# In[9]:


mtse.execute_query(f"""
delete from {table_orgaos_partidarios}
where orgao_partidario_id in (
select orgao_partidario_id from (
select orgao_partidario_id,count(*) qtd from {table_orgaos_partidarios} op 
group by orgao_partidario_id
) t
where qtd >1
)
and esfera_partidaria_cd = 'E'
;
"""
                  )


# In[10]:


mtse.pandas_query(f"""
select orgao_partidario_id, qtd from (
select orgao_partidario_id,count(*) qtd from {table_orgaos_partidarios} op 
group by orgao_partidario_id
) t
where qtd >1
;
""")


# In[11]:


mtse.execute_query(f"""
delete from {table_orgaos_partidarios}
where orgao_partidario_id = 'OP00509018000113'
and {ano_eleicao} ='2018'
;

insert into  {table_orgaos_partidarios}
(
    orgao_partidario_id,
    cnpj                ,
    nome                ,
    uf                  ,
    ue                  ,
    ue_nome             ,
    partido_sg          ,
    partido_nr          ,
    esfera_partidaria_cd   ,
    esfera_partidaria_ds   ,
    esfera_partidaria_sg  ,
    label               
 )
values (
    'OP00509018000113',
    '00509018000113',
    'TRIBUNAL SUPERIOR ELEITORAL',
    'BR',
    '-1',
    '#NULO#',
    '#NULO#',
    '#NULO#',
    'F',
    '#NULO#',
    '#NULO#',
    'TRIBUNAL SUPERIOR ELEITORAL'
)
;
"""
)


# In[12]:


mtse.pandas_query(f"""
select orgao_partidario_id from (
select orgao_partidario_id,count(*) qtd from {table_orgaos_partidarios} op 
group by orgao_partidario_id
) t
where qtd >1
;
""")


# In[13]:


update_efera_partidaria = f"""
update {table_orgaos_partidarios} as op
    set esfera_partidaria_sg = sg,
        esfera_partidaria_ds = ds,
        esfera_partidaria_cd = cd
from {table_esferas_partidarias} as ep
where 
      (
      op.nome                 = ep.ds 
      or
      op.nome                 = ep.tipo
      )
      or
      (
      op.esfera_partidaria_ds = ep.ds
      or
      op.esfera_partidaria_ds = ep.tipo
      ) 
;
"""
mtse.execute_query(update_efera_partidaria)


# In[14]:


update_label = f"""
update {table_orgaos_partidarios}
    set label = get_orgao_partidario_label(partido_sg,uf,esfera_partidaria_sg,ue_nome) 
;
"""
mtse.execute_query(update_label)


# In[15]:


import datetime
print(datetime.datetime.now())


# In[ ]:





# In[ ]:




