#!/usr/bin/env python
# coding: utf-8

# # 020-rede2014_converte_receitas

# In[1]:


ano_eleicao = '2014'


# In[2]:


dbschema = f'rede{ano_eleicao}'
dbschema_tse = f'tse{ano_eleicao}'
table_receitas = f'{dbschema}.receitas_{ano_eleicao}'
table_receitas_do = f'{dbschema}.receitas_do_{ano_eleicao}'
table_receitas_candidatos = f'{dbschema_tse}.receitas_candidatos_{ano_eleicao}'
table_receitas_partidos = f'{dbschema_tse}.receitas_partidos_{ano_eleicao}'
table_receitas_comites = f'{dbschema_tse}.receitas_comites_{ano_eleicao}'
table_candidaturas =  f"{dbschema}.candidaturas_{ano_eleicao}"


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

    {ano_eleicao}                as       eleicao_ano,
    '#NE'                        as       eleicao_turno,
    '#NE'                        as       prestador_contas_sq,
    cnpj_prestador_conta         as       prestador_contas_cnpj,
    
              --RECEITA  
    '#NE'                        as       receita_fonte_cd,
    fonte_recurso                as       receita_fonte_ds,
    '#NE'                        as       receita_origem_cd,
    '#NE'                        as       receita_origem_sg,
    tipo_receita                 as       receita_origem_ds,
    valor_receita::numeric(18,2)    as       receita_valor,
    
              --RECEPTOR  
    get_candidato_id(cpf_do_candidato) as       receptor_id,
    'CD'                         as       receptor_tipo_cd,
    'Candidato'                  as       receptor_tipo_ds,
    get_candidatura_id(uf,numero_candidato) as       receptor_candidatura_id,
    
    '#NE'                        as       receptor_esfera_partidaria_cd,
    '#NE'                        as       receptor_esfera_partidaria_ds,
    uf                           as       receptor_uf,
    '#NE'                        as       receptor_ue,
    '#NE'                        as       receptor_ue_nome,
    cnpj_prestador_conta         as       receptor_cnpj,
    nome_candidato               as       receptor_nome,
    sequencial_candidato         as       receptor_sq,
    cargo                        as       receptor_cargo_ds,
    numero_candidato             as       receptor_candidato_nr,
    cpf_do_candidato             as       receptor_candidato_cpf,
    '#NE'                        as       receptor_vice_candidato_cpf,
    '#NE'                        as       receptor_partido_nr,
    sigla__partido               as       receptor_partido_sg,
    
              --DOADOR  
    get_doador_id(cpf_cnpj_do_doador)           as       doador_id,   
    get_doador_tipo(cpf_cnpj_do_doador)         as       doador_tipo_cd,
    get_doador_tipo_ds(cpf_cnpj_do_doador)      as       doador_tipo_ds,  
    ''                                          as       doador_candidatra_id,  
    
    cod_setor_economico_do_doador               as       doador_cnae_cd,
    setor_economico_do_doador                   as       doador_cnae_ds,
    cpf_cnpj_do_doador                          as       doador_cpf_cnpj,
    nome_do_doador                              as       doador_nome,
    nome_do_doador_receita_federal              as       doador_nome_rfb,
    '#NE'                                       as       doador_originario_tipo,
    '#NE'                                       as       doador_esfera_partidaria_cd,  
    '#NE'                                       as       doador_esfera_partidaria_ds,
    '#NE'                                       as       doador_uf, 
    sigla_ue_doador                             as       doador_ue,
    '#NE'                                       as       doador_ue_nome,
    numero_candidato_doador                     as       doador_candidato_nr,
    '#NE'                                       as       doador_candidato_cargo_ds,
    numero_partido_doador                       as       doador_partido_nr,
    '#NE'                                       as       doador_partido_sg
from {table_receitas_candidatos}
;
"""

mtse.execute_query(query_insert_receitas_candidatos)


# ## INCLUI RECEITAS DE CANDIDATOS DOADOR ORIGINÁRIO

# In[7]:


query_insert_receitas_candidatos_doador_originario = f"""
insert into {table_receitas_do}
select   
    'RCDO'                       as       tabela_id,
        
    {ano_eleicao}                as       eleicao_ano,
    '#NE'                        as       eleicao_turno,
    '#NE'                        as       prestador_contas_sq,
    cnpj_prestador_conta         as       prestador_contas_cnpj,
    
              --RECEITA  
    '#NE'                        as       receita_fonte_cd,
    fonte_recurso                as       receita_fonte_ds,
    '#NE'                        as       receita_origem_cd,
    '#NE'                        as       receita_origem_sg,
    tipo_receita                 as       receita_origem_ds,
    valor_receita::numeric(18,2) as       receita_valor,
    
              --RECEPTOR  
    get_candidato_id(cpf_do_candidato) as       receptor_id,
    'CD'                         as       receptor_tipo_cd,
    'Candidato'                  as       receptor_tipo_ds,
    ''                           as       receptor_candidatura_id,
    
    '#NE'                        as       receptor_esfera_partidaria_cd,
    '#NE'                        as       receptor_esfera_partidaria_ds,
    uf                           as       receptor_uf,
    '#NE'                        as       receptor_ue,
    '#NE'                        as       receptor_ue_nome,
    cnpj_prestador_conta         as       receptor_cnpj,
    nome_candidato               as       receptor_nome,
    sequencial_candidato         as       receptor_sq,
    cargo                        as       receptor_cargo_ds,
    numero_candidato             as       receptor_candidato_nr,
    cpf_do_candidato             as       receptor_candidato_cpf,
    '#NE'                        as       receptor_vice_candidato_cpf,
    '#NE'                        as       receptor_partido_nr,
    sigla__partido               as       receptor_partido_sg,
  
              --DOADOR            
    get_doador_id(cpf_cnpj_do_doador_originario)       as     doador_id,
    get_doador_tipo(cpf_cnpj_do_doador_originario)     as     doador_tipo_cd,
    get_doador_tipo_ds(cpf_cnpj_do_doador_originario)  as     doador_tipo_ds,
    ''                                                 as     doador_candidatura_id,   
    
    '#NE'                                              as     doador_cnae_cd,
    setor_economico_do_doador_originario               as     doador_cnae_ds,
    cpf_cnpj_do_doador_originario                      as     doador_cpf_cnpj,
    nome_do_doador_originario                          as     doador_nome,
    nome_do_doador_originario_receita_federal          as     doador_nome_rfb,
    tipo_doador_originario                             as     doador_originario_tipo,
    '#NE'                                              as     doador_esfera_partidaria_cd,  
    '#NE'                                              as     doador_esfera_partidaria_ds,
    '#NE'                                              as     doador_uf, 
    sigla_ue_doador                                    as     doador_ue,
    '#NE'                                              as     doador_ue_nome,
    numero_candidato_doador                            as     doador_candidato_nr,
    '#NE'                                              as     doador_candidato_cargo_ds,
    numero_partido_doador                              as     doador_partido_nr,
    '#NE'                                              as     doador_partido_sg
from {table_receitas_candidatos} 
where 
 cpf_cnpj_do_doador_originario not in ('#NE','#NE#', '','#NULO','#NULO#')
;
"""

mtse.execute_query(query_insert_receitas_candidatos_doador_originario)


# ## INCLUI RECEITAS PARTIDOS

# In[8]:


query_insert_receitas_partidos = f"""
insert into {table_receitas}
select 
   'ROP'                            as           tabela_id,       

    {ano_eleicao}                   as           eleicao_ano,
    '#NE'                           as           eleicao_turno,
    '#NE'                           as           prestador_contas_sq,
    cnpj_prestador_conta            as           prestador_contas_cnpj,
    
                 --RECEITA           
    '#NE'                           as       receita_fonte_cd,
    fonte_recurso                   as       receita_fonte_ds,
    '#NE'                           as       receita_origem_cd,
    '#NE'                           as       receita_origem_sg,
    tipo_receita                    as       receita_origem_ds,
    valor_receita::numeric(18,2)    as       receita_valor,  
    
                 --RECEPTOR 
    public.get_orgao_partidario_id(cnpj_prestador_conta) as receptor_id,
    'OP'                            as           receptor_tipo_cd,
    'Órgão Partidário'              as           receptor_tipo_ds,
    ''                              as           receptor_candidatura_id,
    
    '#NE'                           as           receptor_esfera_partidaria_cd,
    '#NE'                           as           receptor_esfera_partidaria_ds,
    uf                              as           receptor_uf,
    '#NE'                           as           receptor_ue,
    '#NE'                           as           receptor_ue_nome,
    cnpj_prestador_conta            as           receptor_cnpj,
    tipo_diretorio                  as           receptor_nome,
    sequencial_diretorio            as           receptor_sq,
    '#NE'                           as           receptor_cargo_ds,
    '#NE'                           as           receptor_candidato_nr,
    '#NE'                           as           receptor_candidato_cpf,
    '#NE'                           as           receptor_vice_candidato_cpf,
    '#NE'                           as           receptor_partido_nr,
    sigla__partido                  as           receptor_partido_sg,
    
                 --DOADOR  
    get_doador_id(cpf_cnpj_do_doador)           as       doador_id,   
    get_doador_tipo(cpf_cnpj_do_doador)         as       doador_tipo_cd,
    get_doador_tipo_ds(cpf_cnpj_do_doador)      as       doador_tipo_ds,    
    ''                                          as       doador_candidatura_id,    

    cod_setor_economico_do_doador               as       doador_cnae_cd,
    setor_economico_do_doador                   as       doador_cnae_ds,
    cpf_cnpj_do_doador                          as       doador_cpf_cnpj,
    nome_do_doador                              as       doador_nome,
    nome_do_doador_receita_federal              as       doador_nome_rfb,
    '#NE'                                       as       doador_originario_tipo,
    '#NE'                                       as       doador_esfera_partidaria_cd,  
    '#NE'                                       as       doador_esfera_partidaria_ds,
    '#NE'                                       as       doador_uf, 
    sigla_ue_doador                             as       doador_ue,
    '#NE'                                       as       doador_ue_nome,
    numero_candidato_doador                     as       doador_candidato_nr,
    '#NE'                                       as       doador_candidato_cargo_ds,
    numero_partido_doador                       as       doador_partido_nr,
    '#NE'                                       as       doador_partido_sg              
from {table_receitas_partidos}
--where 
-- cpf_cnpj_do_doador_originario in ('#NE','#NE#', '','#NULO','#NULO#')
;
"""

mtse.execute_query(query_insert_receitas_partidos)


# ## INCLUI RECEITAS PARTIDOS DOADOR ORIGINÁRIO

# In[9]:


query_insert_receitas_partidos_doador_originario = f"""
insert into {table_receitas_do}
select
    'ROPDO'                    as       tabela_id,

    {ano_eleicao}                   as           eleicao_ano,
    '#NE'                           as           eleicao_turno,
    '#NE'                           as           prestador_contas_sq,
    cnpj_prestador_conta            as           prestador_contas_cnpj,
    
                 --RECEITA           
    '#NE'                           as       receita_fonte_cd,
    fonte_recurso                   as       receita_fonte_ds,
    '#NE'                           as       receita_origem_cd,
    '#NE'                           as       receita_origem_sg,
    tipo_receita                    as       receita_origem_ds,
    valor_receita::numeric(18,2)    as       receita_valor,  
    
                 --RECEPTOR 
    public.get_orgao_partidario_id(cnpj_prestador_conta) as receptor_id,
    'OP'                            as           receptor_tipo_cd,
    'Órgão Partidário'              as           receptor_tipo_ds,
    ''                              as           receptor_candidatura_id,
    
    '#NE'                           as           receptor_esfera_partidaria_cd,
    '#NE'                           as           receptor_esfera_partidaria_ds,
    uf                              as           receptor_uf,
    '#NE'                           as           receptor_ue,
    '#NE'                           as           receptor_ue_nome,
    cnpj_prestador_conta            as           receptor_cnpj,
    tipo_diretorio                  as           receptor_nome,
    sequencial_diretorio            as           receptor_sq,
    '#NE'                           as           receptor_cargo_ds,
    '#NE'                           as           receptor_candidato_nr,
    '#NE'                           as           receptor_candidato_cpf,
    '#NE'                           as           receptor_vice_candidato_cpf,
    '#NE'                           as           receptor_partido_nr,
    sigla__partido                  as           receptor_partido_sg,
    
                --DOADOR 
    get_doador_id(cpf_cnpj_do_doador_originario)       as     doador_id,
    get_doador_tipo(cpf_cnpj_do_doador_originario)     as     doador_tipo_cd,
    get_doador_tipo_ds(cpf_cnpj_do_doador_originario)  as     doador_tipo_ds,
    ''                                                 as     doador_candidatura_id,    

    '#NE'                                              as     doador_cnae_cd,
    setor_economico_do_doador_originario               as     doador_cnae_ds,
    cpf_cnpj_do_doador_originario                      as     doador_cpf_cnpj,
    nome_do_doador_originario                          as     doador_nome,
    nome_do_doador_originario_receita_federal          as     doador_nome_rfb,
    tipo_doador_originario                             as     doador_originario_tipo,
    '#NE'                                              as     doador_esfera_partidaria_cd,  
    '#NE'                                              as     doador_esfera_partidaria_ds,
    '#NE'                                              as     doador_uf, 
    sigla_ue_doador                                    as     doador_ue,
    '#NE'                                              as     doador_ue_nome,
    numero_candidato_doador                            as     doador_candidato_nr,
    '#NE'                                              as     doador_candidato_cargo_ds,
    numero_partido_doador                              as     doador_partido_nr,
    '#NE'                                              as     doador_partido_sg
from {table_receitas_partidos} as rpdo
where 
 cpf_cnpj_do_doador_originario not in ('#NE','#NE#', '','#NULO','#NULO#')
;
"""

mtse.execute_query(query_insert_receitas_partidos_doador_originario)


# ## INCLUI RECEITAS COMITES

# In[10]:


query_insert_receitas_comites = f"""
insert into {table_receitas}
select 
   'ROP'                            as           tabela_id,       

    {ano_eleicao}                   as           eleicao_ano,
    '#NE'                           as           eleicao_turno,
    '#NE'                           as           prestador_contas_sq,
    cnpj_prestador_conta            as           prestador_contas_cnpj,
    
                 --RECEITA           
    '#NE'                           as       receita_fonte_cd,
    fonte_recurso                   as       receita_fonte_ds,
    '#NE'                           as       receita_origem_cd,
    '#NE'                           as       receita_origem_sg,
    tipo_receita                    as       receita_origem_ds,
    valor_receita::numeric(18,2)    as       receita_valor,  
    
                 --RECEPTOR 
    public.get_orgao_partidario_id(cnpj_prestador_conta) as receptor_id,
    'OP'                            as           receptor_tipo_cd,
    'Órgão Partidário'              as           receptor_tipo_ds,
    ''                              as           receptor_candidatura_id,
    
    '#NE'                           as           receptor_esfera_partidaria_cd,
    '#NE'                           as           receptor_esfera_partidaria_ds,
    uf                              as           receptor_uf,
    '#NE'                           as           receptor_ue,
    '#NE'                           as           receptor_ue_nome,
    cnpj_prestador_conta            as           receptor_cnpj,
    tipo_comite                     as           receptor_nome,
    sequencial_comite               as           receptor_sq,
    '#NE'                           as           receptor_cargo_ds,
    '#NE'                           as           receptor_candidato_nr,
    '#NE'                           as           receptor_candidato_cpf,
    '#NE'                           as           receptor_vice_candidato_cpf,
    '#NE'                           as           receptor_partido_nr,
    sigla__partido                  as           receptor_partido_sg,
    
                 --DOADOR  
    get_doador_id(cpf_cnpj_do_doador)           as       doador_id,   
    get_doador_tipo(cpf_cnpj_do_doador)         as       doador_tipo_cd,
    get_doador_tipo_ds(cpf_cnpj_do_doador)      as       doador_tipo_ds, 
    ''                                          as       doador_candidatura_id,
    
    cod_setor_economico_do_doador               as       doador_cnae_cd,
    setor_economico_do_doador                   as       doador_cnae_ds,
    cpf_cnpj_do_doador                          as       doador_cpf_cnpj,
    nome_do_doador                              as       doador_nome,
    nome_do_doador_receita_federal              as       doador_nome_rfb,
    '#NE'                                       as       doador_originario_tipo,
    '#NE'                                       as       doador_esfera_partidaria_cd,  
    '#NE'                                       as       doador_esfera_partidaria_ds,
    '#NE'                                       as       doador_uf, 
    sigla_ue_doador                             as       doador_ue,
    '#NE'                                       as       doador_ue_nome,
    numero_candidato_doador                     as       doador_candidato_nr,
    '#NE'                                       as       doador_candidato_cargo_ds,
    numero_partido_doador                       as       doador_partido_nr,
    '#NE'                                       as       doador_partido_sg              
from {table_receitas_comites}
--where 
-- cpf_cnpj_do_doador_originario in ('#NE','#NE#', '','#NULO','#NULO#')
;
"""

mtse.execute_query(query_insert_receitas_comites)


# ## INCLUI RECEITAS COMITES DOADOR ORIGINÁRIO

# In[11]:


query_insert_receitas_comites_doador_originario = f"""
insert into {table_receitas_do}
select
    'ROPDO'                         as       tabela_id,

    {ano_eleicao}                   as           eleicao_ano,
    '#NE'                           as           eleicao_turno,
    '#NE'                           as           prestador_contas_sq,
    cnpj_prestador_conta            as           prestador_contas_cnpj,
    
                --RECEITA           
    '#NE'                           as           receita_fonte_cd,
    fonte_recurso                   as           receita_fonte_ds,
    '#NE'                           as           receita_origem_cd,
    '#NE'                           as           receita_origem_sg,
    tipo_receita                    as           receita_origem_ds,
    valor_receita::numeric(18,2)    as           receita_valor,  
    
                --RECEPTOR 
    public.get_orgao_partidario_id(cnpj_prestador_conta) as receptor_id,
    'OP'                            as           receptor_tipo_cd,
    'Órgão Partidário'              as           receptor_tipo_ds,
    ''                              as           receptor_candidatura_id,
    
    '#NE'                           as           receptor_esfera_partidaria_cd,
    '#NE'                           as           receptor_esfera_partidaria_ds,
    uf                              as           receptor_uf,
    '#NE'                           as           receptor_ue,
    '#NE'                           as           receptor_ue_nome,
    cnpj_prestador_conta            as           receptor_cnpj,
    tipo_comite                     as           receptor_nome,
    sequencial_comite               as           receptor_sq,
    '#NE'                           as           receptor_cargo_ds,
    '#NE'                           as           receptor_candidato_nr,
    '#NE'                           as           receptor_candidato_cpf,
    '#NE'                           as           receptor_vice_candidato_cpf,
    '#NE'                           as           receptor_partido_nr,
    sigla__partido                  as           receptor_partido_sg,
    
               --DOADOR 
    get_doador_id(cpf_cnpj_do_doador_originario)       as     doador_id,
    get_doador_tipo(cpf_cnpj_do_doador_originario)     as     doador_tipo_cd,
    get_doador_tipo_ds(cpf_cnpj_do_doador_originario)  as     doador_tipo_ds,
    ''                                                 as     doador_candiatura_id,
    
    '#NE'                                              as     doador_cnae_cd,
    setor_economico_do_doador_originario               as     doador_cnae_ds,
    cpf_cnpj_do_doador_originario                      as     doador_cpf_cnpj,
    nome_do_doador_originario                          as     doador_nome,
    nome_do_doador_originario_receita_federal          as     doador_nome_rfb,
    tipo_doador_originario                             as     doador_originario_tipo,
    '#NE'                                              as     doador_esfera_partidaria_cd,  
    '#NE'                                              as     doador_esfera_partidaria_ds,
    '#NE'                                              as     doador_uf, 
    sigla_ue_doador                                    as     doador_ue,
    '#NE'                                              as     doador_ue_nome,
    numero_candidato_doador                            as     doador_candidato_nr,
    '#NE'                                              as     doador_candidato_cargo_ds,
    numero_partido_doador                              as     doador_partido_nr,
    '#NE'                                              as     doador_partido_sg
from {table_receitas_comites} as rpdo
where 
 cpf_cnpj_do_doador_originario not in ('#NE','#NE#', '','#NULO','#NULO#')
;
"""

mtse.execute_query(query_insert_receitas_comites_doador_originario)


# In[12]:


import datetime
print(datetime.datetime.now())


# In[ ]:





# In[ ]:




