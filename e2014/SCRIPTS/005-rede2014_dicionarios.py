#!/usr/bin/env python
# coding: utf-8

# # 005-dicionarios

# In[ ]:


ano_eleicao = '2014'
dbschema = f'rede{ano_eleicao}'


# In[ ]:


table_origem_receitas =  f'{dbschema}.origem_receitas_{ano_eleicao}'
table_fonte_receitas = f'{dbschema}.fonte_receitas_{ano_eleicao}'
table_esferas_partidarias = f'{dbschema}.esferas_partidarias_{ano_eleicao}'
table_munzona = f"tse{ano_eleicao}.votacao_candidato_munzona_{ano_eleicao}"
table_consulta_cand = f"tse{ano_eleicao}.consulta_cand_{ano_eleicao}"
table_municipios = f"{dbschema}.municipios_{ano_eleicao}"
table_partidos = f"{dbschema}.partidos_{ano_eleicao}"


# In[3]:


import os
import sys
sys.path.append('../')
import mod_tse as mtse
home = os.environ["HOME"]


# In[4]:


mtse.execute_query(f'CREATE SCHEMA IF NOT EXISTS {dbschema};')


# ### DICIONÁRIO ORIGEM DAS RECEITAS

# In[5]:


query_create_table_origem_receitas = f"""
DROP TABLE IF EXISTS {table_origem_receitas} CASCADE;
create table {table_origem_receitas} (
    cd_origem_receita   varchar, -- codigo
    sg_origem_receita   varchar, -- sigla
    tx_origem_receita   varchar,  -- descricao textual
    ds_origem_receita   varchar   -- descricao padronizada
    
);

CREATE INDEX ON {table_origem_receitas} (cd_origem_receita);
CREATE INDEX ON {table_origem_receitas} (sg_origem_receita);
CREATE INDEX ON {table_origem_receitas} (ds_origem_receita);
CREATE INDEX ON {table_origem_receitas} (tx_origem_receita);

insert into {table_origem_receitas}
VALUES 
('10010100','RP','Recursos próprios','Recursos próprios'), 
('10010200','RPF','Recursos de pessoas físicas','Recursos de pessoas físicas'),
('10010300','RPJ','Recursos de pessoas jurídicas','Recursos de pessoas jurídicas'), -- criado para equalizar com 2008
('10010400','DPI','Doações pela Internet','Doações pela Internet'),
('10010400','DPI', 'RECURSOS DE DOAÇÕES PELA INTERNET','Doações pela Internet'),
('10020000','RPP','Recursos de partido político','Recursos de partido político'),
('10020500','RFC','Recursos de Financiamento Coletivo','Recursos de Financiamento Coletivo'),
('10030100','CBRE','Comercialização de bens ou realização de eventos','Comercialização de bens ou realização de eventos'),
('10030100','CBRE','COMERCIALIZAÇÃO DE BENS E/OU REALIZAÇÃO DE EVENTOS','Comercialização de bens ou realização de eventos'),
('10030200','RAF','Rendimentos de aplicações financeiras','Rendimentos de aplicações financeiras'),
('10030300','RONI','Recursos de origens não identificadas','Recursos de origens não identificadas'),
('10040000','ROC','Recursos de outros candidatos','Recursos de outros candidatos'),
('10040000','ROC','Recursos de outros candidatos/comitês','Recursos de outros candidatos'),  --descricao de origem de receita usada em 2008
('10050000','DRC','DESCRIÇÃO DAS DOAÇÕES RELATIVAS À COMERCIALIZAÇÃO','Doações relativas à comercialização'),
('99999999','RONI','ERRO CD_TITULO ESTIMÁVEL NULL','Recursos de origens não identificadas'),
('#NE','#NE', 'Informação não registrada','Não identificado')
;
"""

mtse.execute_query(query_create_table_origem_receitas)


# In[ ]:





# ### DICIONÁRIO FONTE DAS RECEITAS

# In[6]:


query_create_table_fonte_receitas = f"""
    DROP TABLE IF EXISTS {table_fonte_receitas} CASCADE;
    create table {table_fonte_receitas} (
        cd_fonte_receita   varchar, -- codigo
        sg_fonte_receita   varchar, -- sigla
        tx_fonte_receita   varchar,  -- descricao textual
        ds_fonte_receita   varchar
    );
    CREATE INDEX ON  {table_fonte_receitas}  (cd_fonte_receita);
    CREATE INDEX ON  {table_fonte_receitas}  (sg_fonte_receita);
    CREATE INDEX ON  {table_fonte_receitas}  (ds_fonte_receita);
    CREATE INDEX ON  {table_fonte_receitas}  (tx_fonte_receita);
    

    insert into {table_fonte_receitas}
    VALUES 
    ('0','FP','Fundo Partidario','Fundo Partidario'), 
    ('1','OR','Outros Recursos','Outros Recursos'),
    ('2','FE','Fundo Especial','Fundo Especial'), 
    ('1','OR','Nao especificado','Outros Recursos'),
    ('1','OR','Outros Recursos nao descritos','Outros Recursos')
    ;
    """

mtse.execute_query(query_create_table_fonte_receitas)


# ### DICIONÁRIO DE ESFERAS PARTIDÁRIAS

# In[7]:


query_create_esferas_partidarias = f"""
DROP TABLE IF EXISTS {table_esferas_partidarias} CASCADE;
create table {table_esferas_partidarias} (
    tipo                  varchar, 
    cd                    varchar,
    ds                    varchar,
    sg                    varchar, 
    cd_sg                 varchar
);

CREATE INDEX ON {table_esferas_partidarias} (tipo);
CREATE INDEX ON {table_esferas_partidarias} (cd);
CREATE INDEX ON {table_esferas_partidarias} (ds);
CREATE INDEX ON {table_esferas_partidarias} (sg);
CREATE INDEX ON {table_esferas_partidarias} (cd_sg);

insert into {table_esferas_partidarias} 
VALUES 
    ('Direção Municipal','M','Direção Municipal','DirMun','DM'),
    ('Direção Estadual/Distrital','E','Direção Estadual/Distrital','DirEst/Dist','DE'),
    ('Direção Nacional','F','Direção Nacional','DirNac','DN'),
    ('Federal (Estadual/Distrital)','E','Direção Estadual/Distrital','DirEst','DE'),
    ('Municipal','M','Direção Municipal','DirMun','DM'),
    ('Direção Municipal/Comissão Provisória','M','Direção Municipal','DirMun','DM'),
    ('Nacional','F','Direção Nacional','DirNac','DN'),
    ('Comitê Financeiro Único','M','Comitê Financeiro Único','CFU','CFU'),
    ('Comitê Financeiro Nacional para Presidente da República','F','Comitê Financeiro Nacional para Presidente da República','CF-Pres','CFP'),
    ('Comitê Financeiro Distrital/Estadual para Governador','E','Comitê Financeiro Distrital/Estadual para Governador','CF-Gov','CFG'),
    ('Comitê Financeiro Distrital/Estadual para Senador da República','E','Comitê Financeiro Distrital/Estadual para Senador da República','CF-Sen','CFS'),
    ('Comitê Financeiro Distrital/Estadual para Deputado Federal','E','Comitê Financeiro Distrital/Estadual para Deputado Federal','CF-DepFed','CFDF'),
    ('Comitê Financeiro Distrital/Estadual para Deputado Distrital','E','Comitê Financeiro Distrital/Estadual para Deputado Distrital','CF-DepDist','CFDD'),
    ('Comitê Financeiro Distrital/Estadual para Deputado Estadual','E','Comitê Financeiro Distrital/Estadual para Deputado Estadual','CF-DepEst','CFDE')
;
"""

mtse.execute_query(query_create_esferas_partidarias)


# ### DICIONÁRIO DE MUNICIPIOS

# In[8]:


create_table_municipios = f"""
DROP TABLE IF EXISTS {table_municipios} CASCADE;
CREATE TABLE {table_municipios} (
    sg_uf varchar,
    cd_municipio varchar,
    nm_municipio varchar,
    rede         varchar
);

insert into {table_municipios}
select
    sigla_uf,
    codigo_municipio,
    nome_municipio,
    'N'
from {table_munzona}
group by
    sigla_uf,
    codigo_municipio,
    nome_municipio
;

CREATE INDEX ON {table_municipios} (sg_uf);
CREATE INDEX ON {table_municipios} (cd_municipio);
CREATE INDEX ON {table_municipios} (nm_municipio);

"""

mtse.execute_query(create_table_municipios)


# ### DICIONÁRIO DE PARTIDOS

# In[9]:


query_create_table_partidos = f"""
drop table if exists {table_partidos} cascade;

-- Atributos obtidos da tabela do TSE consulta_cand
create table {table_partidos}
(
nr_partido varchar,
sg_partido varchar,
nm_partido varchar
);

CREATE INDEX ON {table_partidos} (nr_partido);
CREATE INDEX ON {table_partidos} (sg_partido);
CREATE INDEX ON {table_partidos} (nm_partido);

insert into {table_partidos} 
( select nr_partido,sg_partido,nm_partido 
  from {table_consulta_cand}
  group by nr_partido,sg_partido,nm_partido  )
;
"""
mtse.execute_query(query_create_table_partidos)


# In[10]:


import datetime
print(datetime.datetime.now())

