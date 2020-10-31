#!/usr/bin/env python
# coding: utf-8

# # 020-rede2018_converte_receitas

# In[ ]:


ano_eleicao = '2018'


# In[ ]:


dbschema = f'rede{ano_eleicao}'
dbschema_tse = f'tse{ano_eleicao}'
table_receitas = f'{dbschema}.receitas_{ano_eleicao}'
table_receitas_do = f'{dbschema}.receitas_do_{ano_eleicao}'
table_receitas_candidatos = f'{dbschema_tse}.receitas_candidatos_{ano_eleicao}'
table_receitas_candidatos_doador_originario = f'{dbschema_tse}.receitas_candidatos_doador_originario_{ano_eleicao}'
table_receitas_orgaos_partidarios = f'{dbschema_tse}.receitas_orgaos_partidarios_{ano_eleicao}'
table_receitas_orgaos_partidarios_doador_originario= f'{dbschema_tse}.receitas_orgaos_partidarios_doador_originario_{ano_eleicao}'


# In[3]:


import os
import sys
sys.path.append('../')
import mod_tse as mtse
home = os.environ["HOME"]


# In[4]:


mtse.execute_query(f'CREATE SCHEMA IF NOT EXISTS {dbschema};')


# ## CRIA TABELA RECEITAS

# In[5]:


query_create_table_receitas = f"""
DROP TABLE IF EXISTS {table_receitas} CASCADE;
CREATE TABLE {table_receitas} (
    tabela_id                      varchar,

    eleicao_ano                    varchar,
    eleicao_turno                  varchar,
    prestador_contas_sq            varchar,
    prestador_contas_cnpj          varchar,

              --RECEITA
    receita_fonte_cd               varchar,
    receita_fonte_ds               varchar,
    receita_origem_cd              varchar,
    receita_origem_sg              varchar,
    receita_origem_ds              varchar,
    receita_valor                  numeric(18,2),

              --RECEPTOR
    receptor_id                    varchar,
    receptor_tipo_cd               varchar,
    receptor_tipo_ds               varchar,
    receptor_candidatura_id        varchar,
    
    receptor_esfera_partidaria_cd  varchar,
    receptor_esfera_partidaria_ds  varchar,
    receptor_uf                    varchar,
    receptor_ue                    varchar,
    receptor_ue_nome               varchar,
    receptor_cnpj                  varchar,
    receptor_nome                  varchar,
    receptor_sq                    varchar,
    receptor_cargo_ds              varchar,
    receptor_candidato_nr          varchar,
    receptor_candidato_cpf         varchar,
    receptor_vice_candidato_cpf    varchar,
    receptor_partido_nr            varchar,
    receptor_partido_sg            varchar,

              --DOADOR
    doador_id                      varchar,
    doador_tipo_cd                 varchar,
    doador_tipo_ds                 varchar,
    doador_candidatura_id          varchar,
    
    doador_cnae_cd                 varchar,
    doador_cnae_ds                 varchar,
    doador_cpf_cnpj                varchar,
    doador_nome                    varchar,
    doador_nome_rfb                varchar,
    doador_originario_tipo         varchar,
    doador_esfera_partidaria_cd    varchar,  
    doador_esfera_partidaria_ds    varchar,
    doador_uf                      varchar,
    doador_ue                      varchar,
    doador_ue_nome                 varchar,
    doador_candidato_nr            varchar,
    doador_candidato_cargo_ds      varchar,
    doador_partido_nr              varchar,
    doador_partido_sg              varchar 
);

CREATE INDEX ON {table_receitas} (tabela_id);
CREATE INDEX ON {table_receitas} (receita_origem_sg);
CREATE INDEX ON {table_receitas} (receita_origem_cd);
CREATE INDEX ON {table_receitas} (receita_origem_ds);


CREATE INDEX ON {table_receitas} (receptor_id);
CREATE INDEX ON {table_receitas} (receptor_candidatura_id);
CREATE INDEX ON {table_receitas} (receptor_tipo_cd);
CREATE INDEX ON {table_receitas} (receptor_tipo_ds);
CREATE INDEX ON {table_receitas} (receptor_cnpj);
CREATE INDEX ON {table_receitas} (receptor_candidato_cpf);
CREATE INDEX ON {table_receitas} (receptor_uf); 
CREATE INDEX ON {table_receitas} (receptor_candidato_nr); 
CREATE INDEX ON {table_receitas} (receptor_sq);


CREATE INDEX ON {table_receitas} (doador_id);
CREATE INDEX ON {table_receitas} (doador_candidatura_id);
CREATE INDEX ON {table_receitas} (doador_tipo_cd);
CREATE INDEX ON {table_receitas} (doador_tipo_ds);
CREATE INDEX ON {table_receitas} (doador_cpf_cnpj);
CREATE INDEX ON {table_receitas} (doador_nome);
CREATE INDEX ON {table_receitas} (doador_nome_rfb);


DROP TABLE IF EXISTS {table_receitas_do} CASCADE;
CREATE TABLE {table_receitas_do} (
LIKE {table_receitas}
);


"""

mtse.execute_query(query_create_table_receitas)


# ## INCLUI RECEITAS DE CANDIDATOS

# In[6]:


query_insert_receitas_candidatos = f"""
insert into {table_receitas}
select  
    'RC'                         as       tabela_id,

    ano_eleicao                  as       eleicao_ano,
    st_turno                     as       eleicao_turno,
    sq_prestador_contas          as       prestador_contas_sq,
    nr_cnpj_prestador_conta      as       prestador_contas_cnpj,
    
    --RECEITA  
    cd_fonte_receita             as       receita_fonte_cd,
    ds_fonte_receita             as       receita_fonte_ds,
    cd_origem_receita            as       receita_origem_cd,
    '#NE'                        as       receita_origem_sg,
    ds_origem_receita            as       receita_origem_ds,
    vr_receita::numeric(18,2)    as       receita_valor,
    
              --RECEPTOR 
    get_candidato_id(nr_cpf_candidato)             as       receptor_id,
    'CD'                         as       receptor_tipo_cd,
    'Candidato'                  as       receptor_tipo_ds,
    get_candidatura_id(sg_uf,nr_candidato)         as       receptor_candidatura_id,
    
    '#NE'                        as       receptor_esfera_partidaria_cd,
    '#NE'                        as       receptor_esfera_partidaria_ds,
    sg_uf                        as       receptor_uf,
    sg_ue                        as       receptor_ue,
    nm_ue                        as       receptor_ue_nome,
    nr_cnpj_prestador_conta      as       receptor_cnpj,
    nm_candidato                 as       receptor_nome,
    sq_candidato                 as       receptor_sq,
    ds_cargo                     as       receptor_cargo_ds,
    nr_candidato                 as       receptor_candidato_nr,
    nr_cpf_candidato             as       receptor_candidato_cpf,
    nr_cpf_vice_candidato        as       receptor_vice_candidato_cpf,
    nr_partido                   as       receptor_partido_nr,
    sg_partido                   as       receptor_partido_sg,
    
              --DOADOR  
    get_doador_id(nr_cpf_cnpj_doador)           as       doador_id,  
    get_doador_tipo(nr_cpf_cnpj_doador)         as       doador_tipo_cd,
    get_doador_tipo_ds(nr_cpf_cnpj_doador)      as       doador_tipo_ds,
    ''                                          as       doador_candidatura_id,
    cd_cnae_doador               as       doador_cnae_cd,
    ds_cnae_doador               as       doador_cnae_ds,
    nr_cpf_cnpj_doador           as       doador_cpf_cnpj,
    nm_doador                    as       doador_nome,
    nm_doador_rfb                as       doador_nome_rfb,
    '#NE'                        as       doador_originario_tipo,
    cd_esfera_partidaria_doador  as       doador_esfera_partidaria_cd,  
    ds_esfera_partidaria_doador  as       doador_esfera_partidaria_ds,
    sg_uf_doador                 as       doador_uf,
    cd_municipio_doador          as       doador_ue,
    nm_municipio_doador          as       doador_ue_nome,
    nr_candidato_doador          as       doador_candidato_nr,
    ds_cargo_candidato_doador    as       doador_candidato_cargo_ds,
    nr_partido_doador            as       doador_partido_nr,
    sg_partido_doador            as       doador_partido_sg
from {table_receitas_candidatos}
;
"""

mtse.execute_query(query_insert_receitas_candidatos)


# ## INCLUI RECEITAS DE CANDIDATOS DOADOR ORIGINÁRIO

# In[7]:


query_insert_receitas_candidatos_doador_originario = f"""
insert into {table_receitas_do}
select   
    'RCDO'                               as           tabela_id,
        
    ano_eleicao                          as           eleicao_ano,
    st_turno                             as           eleicao_turno,
    sq_prestador_contas                  as           prestador_contas_sq,
    '#NE'                                as           prestador_contas_cnpj,
        
               --RECEITA           
    '#NE'                                as           receita_fonte_cd,
    '#NE'                                as           receita_fonte_ds,
    '#NE'                                as           receita_origem_cd,
    '#NE'                                as           receita_origem_sg,
    ds_receita                           as           receita_origem_ds,
    vr_receita::numeric(18,2)            as           receita_valor,
    
              --RECEPTOR 
    ''                                   as           receptor_id,                       
    'CD'                                 as           receptor_tipo_cd,
    'Candidato'                          as           receptor_tipo_ds, 
    ''                                   as           receptor_candidatura_id,
    
    '#NE'                                as           receptor_esfera_partidaria_cd,
    '#NE'                                as           receptor_esfera_partidaria_ds,
    sg_uf                                as           receptor_uf,
    '#NE'                                as           receptor_ue,
    '#NE'                                as           receptor_ue_nome,
    '#NE'                                as           receptor_cnpj,
    '#NE'                                as           receptor_nome,
    '#NE'                                as           receptor_sq,
    '#NE'                                as           receptor_cargo_ds,
    '#NE'                                as           receptor_candidato_nr,
    '#NE'                                as           receptor_candidato_cpf,
    '#NE'                                as           receptor_vice_candidato_cpf,
    '#NE'                                as           receptor_partido_nr,
    '#NE'                                as           receptor_partido_sg,
  
              --DOADOR  
    get_doador_id(nr_cpf_cnpj_doador_originario)       as     doador_id,
    get_doador_tipo(nr_cpf_cnpj_doador_originario)     as     doador_tipo_cd,
    get_doador_tipo_ds(nr_cpf_cnpj_doador_originario)  as     doador_tipo_ds,
    ''                                                 as     doador_candidatura_id,   
    
    cd_cnae_doador_originario            as           doador_cnae_cd,
    ds_cnae_doador_originario            as           doador_cnae_ds,
    nr_cpf_cnpj_doador_originario        as           doador_cpf_cnpj,
    nm_doador_originario                 as           doador_nome,
    nm_doador_originario_rfb             as           doador_nome_rfb,
    tp_doador_originario                 as           doador_originario_tipo,
    '#NE'                                as           doador_esfera_partidaria_cd,  
    '#NE'                                as           doador_esfera_partidaria_ds,
    '#NE'                                as           doador_uf,
    '#NE'                                as           doador_ue,
    '#NE'                                as           doador_ue_nome,
    '#NE'                                as           doador_candidato_nr,
    '#NE'                                as           doador_candidato_cargo_ds,
    '#NE'                                as           doador_partido_nr,
    '#NE'                                as           doador_partido_sg
from {table_receitas_candidatos_doador_originario} as  rcdo
;
"""

mtse.execute_query(query_insert_receitas_candidatos_doador_originario)


# ## INCLUI RECEITAS ORGÃOS PARTIDÁRIOS

# In[8]:


query_insert_receitas_orgaos_partidarios = f"""
insert into {table_receitas}
select 
   'ROP'                            as           tabela_id,       

    ano_eleicao                     as           eleicao_ano,
    '#NE'                           as           eleicao_turno,
    sq_prestador_contas             as           prestador_contas_sq,
    nr_cnpj_prestador_conta         as           prestador_contas_cnpj,
    
    --RECEITA           
    cd_fonte_receita                as           receita_fonte_cd,
    ds_fonte_receita                as           receita_fonte_ds,
    cd_origem_receita               as           receita_origem_cd,
    '#NE'                           as           receita_origem_sg,
    ds_origem_receita               as           receita_origem_ds,
    vr_receita::numeric(18,2)       as           receita_valor,  
    
        --RECEPTOR 
    public.get_orgao_partidario_id(nr_cnpj_prestador_conta) as receptor_id,
    'OP'                            as           receptor_tipo_cd,
    'Órgão Partidário'              as           receptor_tipo_ds,
    ''                              as           receptor_candidatura_id,
    
    cd_esfera_partidaria            as           receptor_esfera_partidaria_cd,
    ds_esfera_partidaria            as           receptor_esfera_partidaria_ds,
    sg_uf                           as           receptor_uf,
    cd_municipio                    as           receptor_ue,
    nm_municipio                    as           receptor_ue_nome,
    nr_cnpj_prestador_conta         as           receptor_cnpj,
    nm_partido                      as           receptor_nome,
    '#NE'                           as           receptor_sq,
    '#NE'                           as           receptor_cargo_ds,
    '#NE'                           as           receptor_candidato_nr,
    '#NE'                           as           receptor_candidato_cpf,
    '#NE'                           as           receptor_vice_candidato_cpf,
    nr_partido                      as           receptor_partido_nr,
    sg_partido                      as           receptor_partido_sg,
    
        --DOADOR  
    get_doador_id(nr_cpf_cnpj_doador)           as       doador_id,   
    get_doador_tipo(nr_cpf_cnpj_doador)         as       doador_tipo_cd,
    get_doador_tipo_ds(nr_cpf_cnpj_doador)      as       doador_tipo_ds,
    ''                                          as       doador_candidatura_id,    

    cd_cnae_doador                  as           doador_cnae_cd,
    ds_cnae_doador                  as           doador_cnae_ds,
    nr_cpf_cnpj_doador              as           doador_cpf_cnpj,
    nm_doador                       as           doador_nome,
    nm_doador_rfb                   as           doador_nome_rfb,
    '#NE'                           as           doador_originario_tipo,
    cd_esfera_partidaria_doador     as           doador_esfera_partidaria_cd,  
    ds_esfera_partidaria_doador     as           doador_esfera_partidaria_ds,
    sg_uf_doador                    as           doador_uf,
    cd_municipio_doador             as           doador_ue,
    nm_municipio_doador             as           doador_ue_nome,
    nr_candidato_doador             as           doador_candidato_nr,
    ds_cargo_candidato_doador       as           doador_candidato_cargo_ds,
    nr_partido_doador               as           doador_partido_nr,
    sg_partido_doador               as           doador_partido_sg
from {table_receitas_orgaos_partidarios}
;
"""

mtse.execute_query(query_insert_receitas_orgaos_partidarios)


# ## INCLUI RECEITAS ORGÃOS PARTIDÁRIOS DOADOR ORIGINÁRIO

# In[9]:


query_insert_receitas_orgaos_partidarios_doador_originario = f"""
insert into {table_receitas_do}
select
    'ROPDO'                    as       tabela_id,

    ano_eleicao                as       eleicao_ano,
    '#NE'                      as       eleicao_turno,
    sq_prestador_contas        as       prestador_contas_sq,
    '#NE'                      as       prestador_contas_cnpj,
    
    --RECEITA   
    '#NE'                      as       receita_fonte_cd,
    '#NE'                      as       receita_fonte_ds,
    '#NE'                      as       receita_origem_cd,
    '#NE'                      as       receita_origem_sg,
    ds_receita                 as       receita_origem_ds,
    vr_receita::numeric(18,2)  as       receita_valor,
    
    --RECEPTOR  
    public.get_orgao_partidario_id(sg_uf)  as receptor_id,
    'OP'                       as       receptor_tipo_cd,
    'Órgão Partidário'         as       receptor_tipo_ds,
    ''                         as           receptor_candidatura_id,
    
    '#NE'                      as       receptor_esfera_partidaria_cd,
    '#NE'                      as       receptor_esfera_partidaria_ds,
     sg_uf                     as       receptor_uf,
    '#NE'                      as       receptor_ue,
    '#NE'                      as       receptor_ue_nome,
    '#NE'                      as       receptor_cnpj,
    '#NE'                      as       receptor_nome,
    '#NE'                      as       receptor_sq,
    '#NE'                      as       receptor_cargo_ds,
    '#NE'                      as       receptor_candidato_nr,
    '#NE'                      as       receptor_candidato_cpf,
    '#NE'                      as       receptor_vice_candidato_cpf,
    '#NE'                      as       receptor_partido_nr,
    '#NE'                      as       receptor_partido_sg,
    
        --DOADOR 
    get_doador_id(nr_cpf_cnpj_doador_originario)       as     doador_id,
    get_doador_tipo(nr_cpf_cnpj_doador_originario)     as     doador_tipo_cd,
    get_doador_tipo_ds(nr_cpf_cnpj_doador_originario)  as     doador_tipo_ds,
    ''                                                 as     doador_candidatura_id,    

    cd_cnae_doador_originario       as       doador_cnae_cd,
    ds_cnae_doador_originario       as       doador_cnae_ds,
    nr_cpf_cnpj_doador_originario   as       doador_cpf_cnpj,
    nm_doador_originario            as       doador_nome,
    nm_doador_originario_rfb        as       doador_nome_rfb,
    tp_doador_originario            as       doador_originario_tipo,
    '#NE'                           as       doador_esfera_partidaria_cd,  
    '#NE'                           as       doador_esfera_partidaria_ds,
    '#NE'                           as       doador_uf,
    '#NE'                           as       doador_ue,
    '#NE'                           as       doador_ue_nome,
    '#NE'                           as       doador_candidato_nr,
    '#NE'                           as       doador_candidato_cargo_ds,
    '#NE'                           as       doador_partido_nr,
    '#NE'                           as       doador_partido_sg
from {table_receitas_orgaos_partidarios_doador_originario} as rpdo
;
"""

mtse.execute_query(query_insert_receitas_orgaos_partidarios_doador_originario)


# ## ATUALIZA DADOS DE RECEPTOR EM DADOR ORIGINÁRIO DE ORGÃOS PARTIDÁRIOS E CANDIDATOS

# In[10]:


query_atualiza_receptor_em_doador_originario = f"""
with receitas as 
(
select 
    tabela_id,
    prestador_contas_sq,
    receptor_id,
    receptor_tipo_cd,
    receptor_tipo_ds,
    receptor_candidatura_id,
    
    receptor_esfera_partidaria_cd,
    receptor_esfera_partidaria_ds,
    receptor_uf,
    receptor_ue,
    receptor_ue_nome,
    receptor_cnpj,
    receptor_cargo_ds,
    receptor_sq,
    receptor_candidato_nr,
    receptor_nome,
    receptor_candidato_cpf,
    receptor_vice_candidato_cpf,
    receptor_partido_nr,
    receptor_partido_sg
from {table_receitas}
where 
    tabela_id in ('ROP','RC')
)
update {table_receitas_do} as rdo
    set 
    receptor_id                    =    r.receptor_id,
    receptor_tipo_cd               =    r.receptor_tipo_cd,
    receptor_tipo_ds               =    r.receptor_tipo_ds,
    receptor_candidatura_id        =    r.receptor_candidatura_id,
    
    receptor_esfera_partidaria_cd  =    r.receptor_esfera_partidaria_cd,
    receptor_esfera_partidaria_ds  =    r.receptor_esfera_partidaria_ds,
    receptor_uf                    =    r.receptor_uf,
    receptor_ue                    =    r.receptor_ue,
    receptor_ue_nome               =    r.receptor_ue_nome,
    receptor_cnpj                  =    r.receptor_cnpj,
    receptor_cargo_ds              =    r.receptor_cargo_ds,
    receptor_sq                    =    r.receptor_sq,
    receptor_candidato_nr          =    r.receptor_candidato_nr,
    receptor_nome                  =    r.receptor_nome,
    receptor_candidato_cpf         =    r.receptor_candidato_cpf,
    receptor_vice_candidato_cpf    =    r.receptor_vice_candidato_cpf,
    receptor_partido_nr            =    r.receptor_partido_nr,
    receptor_partido_sg            =    r.receptor_partido_sg
from receitas as r
  where (r.prestador_contas_sq = rdo.prestador_contas_sq and rdo.tabela_id = 'ROPDO')
     or (r.prestador_contas_sq = rdo.prestador_contas_sq and rdo.tabela_id = 'RCDO')
;
"""

mtse.execute_query(query_atualiza_receptor_em_doador_originario)


# In[11]:


import datetime
print(datetime.datetime.now())


# In[ ]:





# In[ ]:




