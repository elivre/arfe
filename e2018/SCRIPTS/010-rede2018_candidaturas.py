#!/usr/bin/env python
# coding: utf-8

# # 010-candidaturas

# In[ ]:


ano_eleicao = '2018'
dbschema = f'rede{ano_eleicao}'
table_candidaturas =  f"{dbschema}.candidaturas_{ano_eleicao}"
table_consulta_cand = f"tse{ano_eleicao}.consulta_cand_{ano_eleicao}"
table_despesas_candidatos = f"tse{ano_eleicao}.despesas_contratadas_candidatos_{ano_eleicao}"
table_receitas_candidatos = f"tse{ano_eleicao}.receitas_candidatos_{ano_eleicao}"
table_votacao_candidato_munzona  = f"tse{ano_eleicao}.votacao_candidato_munzona_{ano_eleicao}"


# In[2]:


import os
import sys
sys.path.append('../')
import mod_tse as mtse
home = os.environ["HOME"]


# ## TABELA CANDIDATURAS

# In[3]:


query_create_table_candidaturas = F"""
drop table if exists {table_candidaturas} cascade;

-- Atributos obtidos da tabela do TSE consulta_cand
create table {table_candidaturas}
(
    ano_eleicao                   varchar,
    cd_tipo_eleicao               varchar,
    cd_eleicao                    varchar,
    nr_turno                      varchar,
    tp_abrangencia                varchar,
    sg_uf                         varchar,
    sg_ue                         varchar,
    nm_ue                         varchar,
    --------------------------------------
    ds_cargo                      varchar,
    sq_candidato                  varchar,
    nr_candidato                  varchar,
    nm_candidato                  varchar,
    nm_urna_candidato             varchar,
    nr_cpf_candidato              varchar,
    ds_situacao_candidatura       varchar,
    ds_detalhe_situacao_cand      varchar,
    tp_agremiacao                 varchar,
    nr_partido                    varchar,
    sg_partido                    varchar,
    nm_partido                    varchar,
    nm_coligacao                  varchar,
    ds_composicao_coligacao       varchar,
    ds_nacionalidade              varchar,
    sg_uf_nascimento              varchar,
    nm_municipio_nascimento       varchar,
    dt_nascimento                 varchar,
    nr_idade_data_posse           varchar,
    ds_genero                     varchar,
    ds_grau_instrucao             varchar,
    ds_estado_civil               varchar,
    ds_cor_raca                   varchar,
    cd_ocupacao                   varchar,
    ds_ocupacao                   varchar,
    nr_despesa_max_campanha       numeric(18,2),
    ds_sit_tot_turno              varchar,
    st_reeleicao                  varchar,
    st_declarar_bens              varchar,
   ---------------------------------------------
    candidato_id                  varchar,
    candidato_label               varchar,
    candidato_titular_apto        varchar,
    candidatura_id                varchar,
    candidatura_nome              varchar,
    candidatura_label             varchar,
   ---------------------------------------------
    total_votos_turno_1           numeric,
    total_votos_turno_2           numeric,
    total_votos                   numeric,
   ---------------------------------------------    
    nr_cnpj_prestador_conta       varchar,
    declarou_receita              varchar,
    receita_total                 numeric(18,2),
    declarou_despesa              varchar,
    despesa_total                 numeric(18,2),
    custo_voto                    numeric(18,2),
    tse_id                        varchar
 );

CREATE INDEX ON {table_candidaturas} (candidato_id);

CREATE INDEX ON {table_candidaturas} (candidatura_id);

CREATE INDEX ON {table_candidaturas} (nm_candidato);

CREATE INDEX ON {table_candidaturas} (candidato_label);

CREATE INDEX ON {table_candidaturas} (candidatura_label);

CREATE INDEX ON {table_candidaturas} (nm_urna_candidato);

CREATE INDEX  IF NOT EXISTS sq_candidato_idx ON {table_candidaturas} ( sq_candidato );
"""


mtse.execute_query(query_create_table_candidaturas)


# ## Insere os dados de consulta_cand 

# In[4]:


def query_insert_candidaturas(cd_tipo_eleicao, nr_turno):
    query = f"""
    INSERT INTO {table_candidaturas} 
    (SELECT
    ano_eleicao                  as    ano_eleicao, 
    cd_tipo_eleicao              as    cd_tipo_eleicao,
    cd_eleicao                   as    cd_eleicao,
    nr_turno                     as    nr_turno,                       
    tp_abrangencia               as    tp_abrangencia,                       
    sg_uf                        as    sg_uf,                       
    sg_ue                        as    sg_ue,                       
    nm_ue                        as    nm_ue,                                        
    ds_cargo                     as    ds_cargo,                       
    sq_candidato                 as    sq_candidato,                       
    nr_candidato                 as    nr_candidato,                       
    upper(nm_candidato)          as    nm_candidato,                       
    nm_urna_candidato            as    nm_urna_candidato,                                              
    nr_cpf_candidato             as    nr_cpf_candidato,                                                                   
    ds_situacao_candidatura      as    ds_situacao_candidatura,                                              
    ds_detalhe_situacao_cand     as    ds_detalhe_situacao_cand,                       
    tp_agremiacao                as    tp_agremiacao,                       
    nr_partido                   as    nr_partido,                       
    sg_partido                   as    sg_partido,                       
    nm_partido                   as    nm_partido,                                             
    nm_coligacao                 as    nm_coligacao,                       
    ds_composicao_coligacao      as    ds_composicao_coligacao,                                              
    ds_nacionalidade             as    ds_nacionalidade,                       
    sg_uf_nascimento             as    sg_uf_nascimento,                                              
    nm_municipio_nascimento      as    nm_municipio_nascimento,                       
    dt_nascimento                as    dt_nascimento,                       
    nr_idade_data_posse          as    nr_idade_data_posse,                                             
    ds_genero                    as    ds_genero,                                 
    ds_grau_instrucao            as    ds_grau_instrucao,                                             
    ds_estado_civil              as    ds_estado_civil,                                              
    ds_cor_raca                  as    ds_cor_raca,   
    cd_ocupacao                  as    cd_ocupacao,
    ds_ocupacao                  as    ds_ocupacao,                       
    nr_despesa_max_campanha::numeric(18,2)    as    nr_despesa_max_campanha,                                              
    ds_sit_tot_turno             as    ds_sit_tot_turno,                       
    st_reeleicao                 as    st_reeleicao,                       
    st_declarar_bens             as    st_declarar_bens,                                                        
    ---------------------------------------------
    get_candidato_id(nr_cpf_candidato)                                      as    candidato_id,
    get_candidato_label(nm_urna_candidato,ds_cargo,sg_uf,sg_partido)        as    candidato_label, 
    public.eh_candidato_titular_apto(ds_cargo,ds_situacao_candidatura)      as    candidato_titular_apto,
    get_candidatura_id(sg_uf,nr_candidato)                                  as    candidatura_id,
    ''                                                                      as     candidatura_nome,
    ''                                                                      as     candidatura_label, 
    --------------------------------------------
    0                           as     total_votos_turno_1,
    0                           as     total_votos_turno_2,
    0                           as     total_votos,   
    --------------------------------------------
    ''                          as     nr_cnpj_prestador_conta,
    'N'                         as     declarou_receita,
    0                           as     receita_total,
    'N'                         as     declarou_despesa,
    0                           as     despesa_total,
    0                           as     custo_voto,
    get_tse_id(sq_candidato)    as     tse_id
        from
           {table_consulta_cand} as c
        where
            c.cd_tipo_eleicao = '{cd_tipo_eleicao}'
            and c.nr_turno = '{nr_turno}'          
            and get_candidato_id(c.nr_cpf_candidato)||ds_cargo not in (select candidato_id||ds_cargo from {table_candidaturas})
    )
    ;
    """

    mtse.execute_query(query)


# In[5]:


query_insert_candidaturas('2','2')


# In[6]:


query_insert_candidaturas('2','1')


# ### ATUALIZA DADOS 2. TURNO

# In[7]:


q = f"""
update {table_candidaturas} c
     set ds_sit_tot_turno = cc.ds_sit_tot_turno,
         nr_turno = '2'
from (
        select nr_turno,sq_candidato, ds_sit_tot_turno  from {table_consulta_cand}
        where nr_turno = '2'
     ) as cc
where
    c.sq_candidato =  cc.sq_candidato
;

update {table_candidaturas} c
     set ds_sit_tot_turno = 'NÃO ELEITO'
where
     ds_sit_tot_turno = '#NULO#'

"""
mtse.execute_query(q)


# ###  GERA TOTAL RECEITAS A PARTIR DA DECLARAÇÃO DE RECEITAS

# In[8]:


query_update_cnpj_a_partir_receitas = f"""
    with receitas_candidatos as
    (
    SELECT 
      sq_candidato, 
      nr_cnpj_prestador_conta,
      round(sum(vr_receita),2) as receita_total, 
      get_tse_id(sq_candidato) as tse_id 
    FROM 
      {table_receitas_candidatos}
    group by
      sq_candidato, 
      nr_cnpj_prestador_conta,
      tse_id 
    )
    update {table_candidaturas} as c
      set nr_cnpj_prestador_conta = r.nr_cnpj_prestador_conta,
          declarou_receita = 'S',
          receita_total = r.receita_total
    from 
      receitas_candidatos as r
    where
      c.tse_id = r.tse_id 
    ;  
"""

mtse.execute_query(query_update_cnpj_a_partir_receitas)


# ###  GERA TOTAL DESPESAS A PARTIR DA DECLARAÇÃO DE DESPESAS

# In[9]:


query_update_cnpj_a_partir_despesas = f"""
    with despesas_candidatos as
    (
    SELECT 
      sq_candidato, 
      nr_cnpj_prestador_conta,
      round(sum(vr_despesa_contratada),2) as despesa_total,
      get_tse_id(sq_candidato) as tse_id  
    FROM 
      {table_despesas_candidatos}
    group by
      sq_candidato, 
      nr_cnpj_prestador_conta,
      tse_id
    )
    update {table_candidaturas} as c
      set nr_cnpj_prestador_conta = d.nr_cnpj_prestador_conta,
          declarou_despesa = 'S',
          despesa_total = d.despesa_total
    from 
      despesas_candidatos as d
    where
      c.tse_id = d.tse_id
    ;
"""

mtse.execute_query(query_update_cnpj_a_partir_despesas)


# ### Gera total votos turno 1

# In[10]:


query_atualiza_total_votos_turno_1 = f"""
    with votos_turno_1 as 
    (
      select
       get_tse_id(sq_candidato) as tse_id,
       sum(qt_votos_nominais::numeric) as total_votos
      from 
        {table_votacao_candidato_munzona} 
      where 
        nr_turno = '1'
      group by 
        tse_id
    )
    update {table_candidaturas} as c
    set 
       total_votos_turno_1 = v1.total_votos,
       total_votos = v1.total_votos             
    from 
       votos_turno_1 as v1
    where 
       c.tse_id = v1.tse_id
    ;
    """

mtse.execute_query(query_atualiza_total_votos_turno_1)


# ### Gera total votos turno 2

# In[11]:


query_atualiza_total_votos_turno_2 = f"""
    with votos_turno_2 as 
    (
      select
       get_tse_id(sq_candidato) as tse_id,
       sum(qt_votos_nominais::numeric) as total_votos
      from 
        {table_votacao_candidato_munzona} as v2
      where 
        nr_turno = '2'
      group by 
        tse_id
    )
    update {table_candidaturas} as c
    set 
       total_votos_turno_2 = v2.total_votos,
       total_votos         = v2.total_votos
    from 
       votos_turno_2 as v2
    where 
       c.tse_id = v2.tse_id
    ;
"""
    
mtse.execute_query(query_atualiza_total_votos_turno_2)


# ### Cálculo Custo do Voto

# In[12]:


query_calcula_custo_voto = f"""
    update {table_candidaturas}
        set custo_voto = case when total_votos > 0 then round(receita_total / total_votos,2) else 0 end
"""
mtse.execute_query(query_calcula_custo_voto)


# ### Verifica Candidatos com mais de um registro

# In[13]:


mtse.pandas_query(f"""
 select candidato_id, q
 from (select candidato_id, count(*)  as q from {table_candidaturas} 
 group by candidato_id) t
 where q>1 
 order by q desc
 ;
 """
)


# ## Muda o id do registro mais antigo de candidato com mais de um  registro 

# ### exclui candidato_id mais antigo quando dois registros para o mesmo candidato 
# def exclui_duplo_id():
#     p=mtse.pandas_query(f""" 
#     select candidato_id, tse_id from {table_candidaturas}  
#     where candidato_id in(
#      select candidato_id
#      from (select  candidato_id, count(*)  as q from {table_candidaturas}  
#      group by candidato_id) t
#      where q>1
#     ) 
#     order by candidato_id, tse_id
#     ;
#     """
#     )
#     p2=p[['candidato_id','tse_id']]
#     n = p2['candidato_id'].size
#     l=[]
#     for i in range(0,n,2):
#         l.append(p2.iloc[i]['tse_id'])
#     l= "'"+"', '".join(l)+"'"
# 
#     mtse.execute_query(f""" 
#         update {table_candidaturas} 
#            set candidato_id = 
#            case 
#                when candidato_id = 'CD000000000-4' then 'CD'||tse_id
#                else candidato_id||'-I'
#            end
#         where tse_id in ({l}) 
#     """
#     )
#     return(n)
# 
# while True:
#     n=exclui_duplo_id()
#     if n==0:
#         break

# ### Verifica o resultado

# In[14]:


mtse.pandas_query(f""" 
    select candidato_id, tse_id from {table_candidaturas}  
    where candidato_id in(
     select candidato_id
     from (select  candidato_id, count(*)  as q from {table_candidaturas}  
     group by candidato_id) t
     where q>1
    ) 
    order by candidato_id, tse_id
    ;
"""
)


# In[15]:


mtse.pandas_query(f""" 
    select nr_cpf_candidato, candidato_id, tse_id from {table_candidaturas}  
    where nr_cpf_candidato in(
     select nr_cpf_candidato
     from (select  nr_cpf_candidato, count(*)  as q from {table_candidaturas}  
     group by nr_cpf_candidato) t
     where q>1
    ) 
    order by candidato_id, tse_id
    ;
"""
)


# ### ESTABELECE  NOME  E LABEL PARA TODOS OS CANDIDATOS DA MESMA CANDIDATURA (candidatura_id)

# In[16]:


query_update_candidatura_nome_label = f"""
with titulares as
 (
 select * from {table_candidaturas} c 
   where eh_candidato_titular(ds_cargo) = 'S'
 )
 update {table_candidaturas} as  c
     set candidatura_label = get_candidatura_label(t.nm_urna_candidato , t.ds_cargo , t.sg_uf , t.sg_partido ),
         candidatura_nome  = get_candidatura_nome(t.nm_candidato, t.ds_cargo, t.sg_uf, t.sg_partido)
 from titulares as t
 where 
     c.candidatura_id = t.candidatura_id
 ;
"""
mtse.execute_query(query_update_candidatura_nome_label)


# In[ ]:





# In[17]:


mtse.pandas_query(f""" 
    --select candidato_titular_apto, candidatura_id, q from {table_candidaturas}  
    --where candidato_titular_apto||candidatura_id in(
     select c, q 
     from (select  candidato_titular_apto||candidatura_id c , count(*)  as q from {table_candidaturas}  
     where candidato_titular_apto = 'S'
     group by candidato_titular_apto||candidatura_id ) t
     where q>1
   -- ) 
   -- order by candidato_id, tse_id
    ;
"""
)


# In[18]:


mtse.pandas_query(f""" 
    select  candidatura_id, tse_id from {table_candidaturas}  
    where  candidatura_id in(
     select  candidatura_id
     from (
         select   candidatura_id, count(*)  as q from {table_candidaturas} 
         --where candidato_titular_apto = 'S'
         group by candidatura_id
     ) t
     where q>1    
    ) 
    order by  candidatura_id, tse_id
    ;
"""
)


# In[19]:


import pandas as pd 
df_candidaturas_2018=mtse.pandas_query(f""" select sg_uf, ds_cargo, count(*) as  qtd from {table_candidaturas} 
 group by sg_uf, ds_cargo
 order by sg_uf, ds_cargo 
 """)
#df_candidaturas_2018.to_excel('df_candidaturas_2018.xlsx')
df_candidaturas_2018[['sg_uf','ds_cargo']]


# In[20]:


import datetime
print(datetime.datetime.now())


# In[ ]:




