#!/usr/bin/env python
# coding: utf-8

# # rede_gephi

# In[1]:


ano_eleicao = '2018'

rede =f'rede{ano_eleicao}'

csv_dir = f'/home/neilor/{rede}'


# In[2]:


dbschema = f'rede{ano_eleicao}'
table_gephi_edges = f"{dbschema}.gephi_edges_{ano_eleicao}"
table_gephi_nodes = f"{dbschema}.gephi_nodes_{ano_eleicao}"
table_receitas = f"{dbschema}.receitas_{ano_eleicao}"
table_candidaturas = f"{dbschema}.candidaturas_{ano_eleicao}"
table_orgaos_partidarios =  f"{dbschema}.orgaos_partidarios_{ano_eleicao}"
table_municipios = f"{dbschema}.municipios_{ano_eleicao}"


# In[3]:


import sys
sys.path.append('../')
import mod_tse as mtse


# In[4]:


import os
home = os.environ["HOME"]
local_dir = f'{home}/temp'


# In[ ]:





# In[5]:


query_create_gephi_edges = f"""
drop table if exists {table_gephi_edges} cascade;
create table {table_gephi_edges}
(
  Source           varchar,
  Target           varchar,
  Type             varchar,
  "Weight"         numeric(18,2),
  doador_tipo      varchar,
  receptor_tipo    varchar

);

CREATE INDEX ON {table_gephi_edges} (Source);
CREATE INDEX ON {table_gephi_edges} (Target);

"""


mtse.execute_query(query_create_gephi_edges)  


# In[6]:


mtse.execute_query( f"""
insert into {table_gephi_edges}
SELECT
  doador_id           as Source ,
  receptor_id         as Target,
  'undirected'        as Type,
  sum(receita_valor)  as Weight,
  doador_tipo_cd,
  receptor_tipo_cd
FROM {table_receitas}

group by 
  doador_id,
  receptor_id,
  
  doador_tipo_cd,
  receptor_tipo_cd
;
""")


# In[ ]:





# In[7]:


query_create_table_gephi_nodes = f"""
drop table if exists {table_gephi_nodes} cascade;
create table {table_gephi_nodes} (
    id                                      varchar default '',
    label                                   varchar default '',
    tipo                                    varchar default '',
    ue                                      varchar default '',
    nome                                    varchar default '', 
    partido                                 varchar default '',
    esfera_partidaria                       varchar default '',
    candidatura_id                          varchar,
    candidatura_label                       varchar, 
    ------------------------------------------------------     

    ------------------------------------------------------  
    ds_cargo                                varchar,
    nr_candidato                            varchar,
    nm_urna_candidato                       varchar,
    ds_situacao_candidatura                 varchar,
    ds_detalhe_situacao_cand                varchar,
    ds_composicao_coligacao                 varchar,
    ------------------------------------------------------
    total_votos_turno_1                     numeric,
    total_votos_turno_2                     numeric,
    total_votos                             numeric,
    custo_voto                              numeric(18,2),
    ds_sit_tot_turno                        varchar,
    st_reeleicao                            varchar,
   ---------------------------------------------
    valor_recebido                          numeric(18,2) default 0.0,
    valor_doado                             numeric(18,2) default 0.0,
    fonte_fundo_part                        numeric(18,2) default 0.0,
    fonte_fundo_esp                         numeric(18,2) default 0.0,
    fonte_outros_rec                        numeric(18,2) default 0.0,
    RP                                      numeric(18,2) default 0.0,
    RPF                                     numeric(18,2) default 0.0,
    RPJ                                     numeric(18,2) default 0.0,
    DPI                                     numeric(18,2) default 0.0,
    RPP                                     numeric(18,2) default 0.0,
    RFC                                     numeric(18,2) default 0.0,
    CBRE                                    numeric(18,2) default 0.0,
    RAF                                     numeric(18,2) default 0.0,
    RONI                                    numeric(18,2) default 0.0,
    ROC                                     numeric(18,2) default 0.0,
    DRC                                     numeric(18,2) default 0.0,
    ----------------------------------------------     
    declarou_receita              varchar,
    receita_total                 numeric(18,2),
    declarou_despesa              varchar,
    despesa_total                 numeric(18,2),
    ---------------------------------------------
    ds_nacionalidade              varchar,
    sg_uf_nascimento              varchar,
    nm_municipio_nascimento       varchar,
    dt_nascimento                 varchar,
    nr_idade_data_posse           varchar,
    ds_genero                     varchar,
    ds_grau_instrucao             varchar,
    ds_estado_civil               varchar,
    ds_cor_raca                   varchar,
    ds_ocupacao                   varchar

);

CREATE INDEX ON {table_gephi_nodes} (id);

"""

mtse.execute_query(query_create_table_gephi_nodes)


# In[ ]:





# In[8]:


query_inclui_table_gephi_nodes = f"""
insert into {table_gephi_nodes}
select 
    candidato_id               as   id,
    candidato_label            as   label,
    'CD'                       as   tipo,
    sg_ue                      as   ue,
    nm_candidato               as   nome,
    sg_partido                 as   partido,
    get_candidato_esfera_partidaria(ds_cargo) as esfera_paridaria,
    candidatura_id               ,
    candidatura_label            , 
   ------------------------------------------------------     

    ------------------------------------------------------  
    ds_cargo,
    nr_candidato,
    nm_urna_candidato,
    ds_situacao_candidatura,
    ds_detalhe_situacao_cand,
    ds_composicao_coligacao,
    ------------------------------------------------------
    total_votos_turno_1,
    total_votos_turno_2,
    total_votos,
    custo_voto,
    ds_sit_tot_turno,
    st_reeleicao,
   ---------------------------------------------
    0.0                as  valor_recebido,
    0.0                as  valor_doado,
    0.0                as  fonte_fundo_part,
    0.0                as  fonte_fundo_esp,
    0.0                as  fonte_outros_rec,
    0.0                as  RP,
    0.0                as  RPF,
    0.0                as  RPJ,
    0.0                as  DPI,
    0.0                as  RPP,
    0.0                as  RFC,
    0.0                as  CBRE,
    0.0                as  RAF,
    0.0                as  RONI,
    0.0                as  ROC,
    0.0                as  DRC,
    ----------------------------------------------     
    declarou_receita,
    receita_total,
    declarou_despesa,
    despesa_total,
    ---------------------------------------------
    ds_nacionalidade,
    sg_uf_nascimento,
    nm_municipio_nascimento,
    dt_nascimento,
    nr_idade_data_posse,
    ds_genero,
    ds_grau_instrucao,
    ds_estado_civil,
    ds_cor_raca,
    ds_ocupacao              
from {table_candidaturas} cd
;


-- update candidaturas em todos os nos
update {table_gephi_nodes}
      set ds_sit_tot_turno        = c.ds_sit_tot_turno,
          ds_situacao_candidatura = c.ds_situacao_candidatura
from {table_candidaturas} as c
where 
    c.candidato_id  = id
    and
    nr_turno = '2'  
;



"""
mtse.execute_query(query_inclui_table_gephi_nodes) 


# In[9]:


mtse.pandas_query(f"""
 select id, q
 from (select ID, count(*)  as q from {table_gephi_nodes} 
 group by id) t
 where q>1 
 ;
 """
)


# In[10]:


### atualiza os atributos de órgãos partidários na tabela de nós
### com base na tabela de órgãos partidários

query_inclui_orgaos_partidarios = f"""
    insert into {table_gephi_nodes}
    select
        orgao_partidario_id,
        label,
        'OP',
        uf,
        nome,
        partido_sg,
        esfera_partidaria_cd
    from {table_orgaos_partidarios} 
    GROUP BY
        orgao_partidario_id,
        label,
        uf,
        nome,
        partido_sg,
        esfera_partidaria_cd
;
"""

mtse.execute_query(query_inclui_orgaos_partidarios)


# In[11]:


mtse.pandas_query(f"""
 select id, q
 from (select ID, count(*)  as q from {table_gephi_nodes} 
 group by id) t
 where q>1 and id like 'CD%'
 ;
 """
)


# In[12]:


query_inclui_outros_doadores = f"""
    insert into {table_gephi_nodes}
    select
        doador_id      as id,
        case
            when doador_nome_rfb not like '%#NULO%'
                 then doador_nome_rfb ||'('||doador_tipo_cd||')'
            when doador_nome not like '%#NULO%'
                 then doador_nome ||'('||doador_tipo_cd||')'
            else
                '('||doador_tipo_cd||')'
        end            as label,
        doador_tipo_cd as tipo,
        doador_uf      as ue,
        case
            when doador_nome_rfb not like '%#NULO%'
                 then doador_nome_rfb
            when doador_nome not like '%#NULO%'
                 then doador_nome
            else
                '('||doador_tipo_cd||')'
        end           as nome
    from {table_receitas}
    where doador_tipo_cd not in ('CD','OP')
    group by id, label, tipo, ue, nome
    ;
"""
mtse.execute_query(query_inclui_outros_doadores)


# In[13]:


mtse.pandas_query(f"""
 select id, q
 from (select ID, count(*)  as q from {table_gephi_nodes} 
 group by id) t
 where q>1 and id like 'CD%'
 ;
 """
)


# In[14]:


### atualiza total de receita por fonte da receita

def query_update_fonte(fonte):
    q=f"""
    update {table_gephi_nodes} 
        set {fonte[0]} = r.valor
    from (
            select receptor_id, sum(receita_valor) as valor from {table_receitas}
            where 
            receita_fonte_cd = '{fonte[1]}'
            group by receptor_id
         ) as r
    where id = r.receptor_id
    ;
    """
    return q

for f in (['fonte_fundo_part','2'],['fonte_fundo_esp','0'],['fonte_outros_rec','1']):
    q=query_update_fonte(f)
    mtse.execute_query(q)


# In[ ]:





# In[15]:


### atualiza total de receita por origem da receita

def query_update_origem_receita(origem_receita_sg):
    q=f"""
    update {table_gephi_nodes} 
        set {origem_receita_sg} = r.valor
    from (
            select receptor_id, sum(receita_valor) as valor from {table_receitas} 
            where 
            receita_origem_sg = upper('{origem_receita_sg}')
            group by receptor_id 
         ) as r
    where id = r.receptor_id
    ;
    """
    return q

for r in ('RP','RPF','RPJ','DPI','RPP','RFC','CBRE','RAF','RONI','ROC','DRC'):
    mtse.execute_query(query_update_origem_receita(r))


# In[ ]:





# In[16]:


### atualiza total recebido

query_update_valor_recebido = f"""
    update {table_gephi_nodes}
        set VALOR_recebido = valor
    from (
          select receptor_id, sum(receita_valor) as valor from {table_receitas}  r 
          group by receptor_id 
         ) as v
    where id = v.receptor_id
;
"""
mtse.execute_query(query_update_valor_recebido)


# In[ ]:





# In[17]:


query_update_ids = f"""
    with candidatos as
    (
    select candidato_id, candidatura_id from {table_candidaturas}
    where ds_situacao_candidatura = 'APTO'
    )
    update {table_gephi_edges}
    set target = candidatura_id,
        receptor_tipo = 'CA'
    from candidatos as c
    where target = c.candidato_id
    ;
 
     with candidatos as
    (
    select candidato_id, candidatura_id from {table_candidaturas}
    where ds_situacao_candidatura = 'APTO'
    )
    update {table_gephi_edges}
    set source = candidatura_id,
        doador_tipo = 'CA'
    from candidatos as c
    where source = c.candidato_id
    ;
    
    

    with candidatos as
    (
    select candidato_id, candidatura_id, candidatura_label, candidatura_nome from {table_candidaturas}
    where ds_situacao_candidatura = 'APTO'
    )
    update {table_gephi_nodes}
    set id    = candidatura_id,
        label = candidatura_label,
        tipo  = 'CA',
        nome  = candidatura_nome
    from candidatos as c
    where id = c.candidato_id
;
"""
mtse.execute_query(query_update_ids)


# In[ ]:





# In[18]:


### atribui dados do receptor na tabela de arestas

query_join_edges_atributos = f"""
    drop table if exists {table_gephi_edges}_final cascade;

    create table {table_gephi_edges}_final as
    select e.*, n.* 
    from {table_gephi_edges} as e 
    left outer join {table_gephi_nodes} as n  on e.target = n.id
    ;

    CREATE INDEX ON {table_gephi_edges}_final (Source);
    CREATE INDEX ON {table_gephi_edges}_final (Target);
    CREATE INDEX ON {table_gephi_edges}_final (ue);


    ALTER TABLE  {table_gephi_edges}_final
        DROP COLUMN IF EXISTS Id CASCADE,
        DROP COLUMN IF EXISTS tipo CASCADE    
    ;

    drop table if exists {table_gephi_edges} cascade;
    ALTER TABLE IF EXISTS {table_gephi_edges}_final
    RENAME TO gephi_edges_{ano_eleicao};

"""
mtse.execute_query(query_join_edges_atributos)


# In[19]:


import datetime
print(datetime.datetime.now())


# In[ ]:




