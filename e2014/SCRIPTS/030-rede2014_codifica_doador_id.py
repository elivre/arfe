#!/usr/bin/env python
# coding: utf-8

# # 035-rede_codifica_doador_id

# In[1]:


ano_eleicao = '2014'


# In[2]:


dbschema = f'rede{ano_eleicao}'
dbschema_tse = f'tse{ano_eleicao}'

table_receitas = f'{dbschema}.receitas_{ano_eleicao}'

table_receitas_candidatos = f'{dbschema_tse}.receitas_candidatos_{ano_eleicao}'
table_receitas_orgaos_partidarios = f'{dbschema_tse}.receitas_orgaos_partidarios_{ano_eleicao}'
table_consulta_cand = f'{dbschema_tse}.consulta_cand_{ano_eleicao}'

table_candidaturas = f"{dbschema}.candidaturas_{ano_eleicao}"
table_municipios = f"{dbschema}.municipios_{ano_eleicao}"
table_partidos = f'{dbschema}.partidos_{ano_eleicao}'
#table_orgaos_partidarios =  f"{dbschema}.orgaos_partidarios_{ano_eleicao}"
table_origem_receitas = f'{dbschema}.origem_receitas_{ano_eleicao}'
table_fonte_receitas = f'{dbschema}.fonte_receitas_{ano_eleicao}'


# In[ ]:





# In[3]:


import os
import sys
sys.path.append('../')
import mod_tse as mtse
home = os.environ["HOME"]


# In[4]:


mtse.execute_query(f'CREATE SCHEMA IF NOT EXISTS {dbschema};')


# In[5]:


copia_receitas = f"""
DROP TABLE IF EXISTS  {table_receitas} CASCADE;
create table {table_receitas} as
select *  from {table_receitas}_codificada
;
"""
mtse.execute_query(copia_receitas)


# In[ ]:





# In[6]:


query_update_doador_id_cnpj_de_candidato = f"""
with candidatos as
(
select candidato_id, candidatura_id, sg_uf, nr_candidato, nr_cpf_candidato, nm_candidato, ds_cargo ,nr_cnpj_prestador_conta
from {table_candidaturas}
)
update {table_receitas} as r
    set doador_id      = c.candidato_id,
        doador_tipo_cd = 'CD',
        doador_tipo_ds = 'Candidato',
        doador_candidatura_id = c.candidatura_id
    from candidatos as c
    where 
        doador_cpf_cnpj = c.nr_cnpj_prestador_conta
;        
;


with candidatos as
(
select candidato_id, candidatura_id, sg_uf, nr_candidato, nr_cpf_candidato, nm_candidato, ds_cargo ,nr_cnpj_prestador_conta
from {table_candidaturas}
)
update {table_receitas} as r
    set doador_id      = c.candidato_id,
        doador_tipo_cd = 'CD',
        doador_tipo_ds = 'Candidato',
        doador_candidatura_id = c.candidatura_id
    from candidatos as c
    where 
        doador_tipo_cd <> 'CD'
        and
        doador_cpf_cnpj = c.nr_cpf_candidato
;
"""

mtse.execute_query(query_update_doador_id_cnpj_de_candidato)


# In[ ]:





# ### RECEITA DE ORGÃO PARTIDÁRIO

# In[7]:


query_update_doadores_id_origem_RPP_OP_2014= f"""
update {table_receitas}
    set doador_id       =  op.receptor_id,
        doador_tipo_cd  = 'OP',
        doador_tipo_ds  = 'Órgão Partidário' 
    from (select receptor_id,prestador_contas_cnpj from  {table_receitas} where receptor_tipo_cd = 'OP' group by receptor_id,prestador_contas_cnpj) as op
    where
        {ano_eleicao} = '2014'
        and
        receita_origem_sg in ('RPP', 'ROC')
        and 
        doador_tipo_cd = 'PJ'
        and
        doador_cpf_cnpj = op.prestador_contas_cnpj
;
"""

mtse.execute_query(query_update_doadores_id_origem_RPP_OP_2014)


# In[8]:


query_update_doadores_id_origem_RPP_OP_2018= f"""
update {table_receitas}
    set doador_id       =  op.receptor_id,
        doador_tipo_cd  = 'OP',
        doador_tipo_ds  = 'Órgão Partidário' 
    from (select receptor_id,prestador_contas_cnpj from  {table_receitas} where receptor_tipo_cd = 'OP' group by receptor_id,prestador_contas_cnpj) as op
    where
        {ano_eleicao} = '2018'
        and
        receita_origem_sg = 'RPP'
        and 
        doador_tipo_cd = 'PJ'
        and
        doador_cpf_cnpj = op.prestador_contas_cnpj
;
"""

mtse.execute_query(query_update_doadores_id_origem_RPP_OP_2018)


# In[9]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
receita_origem_sg in ('RPP', 'ROC')
and
doador_tipo_cd = 'PJ'""")


# In[10]:


query_update_doador_id_RPP_OP_2014 = f"""

update {table_receitas} as r
    set doador_id      = get_orgao_partidario_id(doador_cpf_cnpj),
        doador_tipo_cd = 'OP',
        doador_tipo_ds = 'Órgão Partidário' 
    where
        {ano_eleicao} = '2014'
        and
        receita_origem_sg in ('RPP', 'ROC')
        and doador_tipo_cd = 'PJ'
        --and upper(doador_nome_rfb) not like '%ELEI%'
        and
        (
               upper(doador_nome_rfb) like '%COMISSAO%'
            or upper(doador_nome_rfb) like '%COMITE%'
            or upper(doador_nome) like '%COMITE%'
            or upper(doador_nome_rfb) like '%DEMOCRATAS%'
            or upper(doador_nome_rfb) like '%DIRECAO%'
            or upper(doador_nome_rfb) like '%DIRETORIO%'
            or upper(doador_nome_rfb) like '%PARTIDO%'
        )    
"""    

mtse.execute_query(query_update_doador_id_RPP_OP_2014)


# In[11]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
receita_origem_sg in ('RPP', 'ROC')
and
doador_tipo_cd = 'PJ'""")


# In[ ]:





# In[12]:


query_update_doador_id_RPP_OP_2018 = f"""

update {table_receitas} as r
    set doador_id      = get_orgao_partidario_id(doador_cpf_cnpj),
        doador_tipo_cd = 'OP',
        doador_tipo_ds = 'Órgão Partidário' 
    where 
        {ano_eleicao} = '2018'
        and
        receita_origem_sg = 'RPP'
        and doador_tipo_cd = 'PJ'
        and upper(doador_nome_rfb) not  like 'ELEI%'
        and 
        (
               upper(doador_nome_rfb)  like '%SUSTENTABILIDADE%'
            or upper(doador_nome_rfb)  like '%PATRIOTA%'
            or upper(doador_nome_rfb)  like '%DEMOCRATAS%'
            or upper(doador_nome_rfb)  like '%PROGRESSISTAS%'
            or upper(doador_nome_rfb)  like '%SOLIDARIEDADE%'
            or upper(doador_nome_rfb)  like '%PODEMOS%'
            or upper(doador_nome_rfb)  like '%AVANTE%'
            or upper(doador_nome_rfb)  like '%TRIBUNAL SUPERIOR%'
            or upper(doador_nome_rfb)  like '%PARTIDO%'
            or upper(doador_nome_rfb)  like '%DIRETORIO%'
            or upper(doador_nome_rfb)  like '%COMISSAO%'
            or upper(doador_nome_rfb)  like '%ORGAO%'
        )    
"""    

mtse.execute_query(query_update_doador_id_RPP_OP_2018)


# In[13]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
receita_origem_sg in ('RPP', 'ROC')
and
doador_tipo_cd = 'PJ'""")


# In[14]:


query_update_doador_id_RPP_OP_2018_2 = f"""

update {table_receitas} as r
    set doador_id      = get_orgao_partidario_id(doador_cpf_cnpj),
        doador_tipo_cd = 'OP',
        doador_tipo_ds = 'Órgão Partidário' 
    where 
        {ano_eleicao} = '2018'
        and
        receita_origem_sg = 'RPP'
        and doador_tipo_cd = 'PJ'
        and doador_nome   like 'Direção%'
"""    

mtse.execute_query(query_update_doador_id_RPP_OP_2018_2)


# In[15]:


query_update_doador_id_RPP_OP_2014_2 = f"""

update {table_receitas} as r
    set doador_id      = get_orgao_partidario_id(doador_cpf_cnpj),
        doador_tipo_cd = 'OP',
        doador_tipo_ds = 'Órgão Partidário' 
    where 
        {ano_eleicao} = '2014'
        and
        receita_origem_sg in ('RPP', 'ROC')
        and doador_tipo_cd = 'PJ'
        and 
        (
        doador_nome   like 'Direção%'
        or
        doador_nome   like 'Comitê%'
        )
"""    

mtse.execute_query(query_update_doador_id_RPP_OP_2014_2)


# In[16]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
receita_origem_sg in ('RPP', 'ROC')
and
doador_tipo_cd = 'PJ'""")


# ### RECEITA DE OUTRO CANDIDATO

# In[17]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
receita_origem_sg = 'ROC'
and
doador_tipo_cd = 'PJ'""")


# ### OUTRO CANDIDATO PESSOA JURÍDICA

# In[18]:


query_update_doador_id_ROC_PJ = f"""
with candidatos as
(
select candidato_id, candidatura_id, sg_uf, nr_candidato, nr_cpf_candidato, nr_cnpj_prestador_conta 
from {table_candidaturas}
)
update {table_receitas} as r
    set doador_id             = c.candidato_id,
        doador_tipo_cd        = 'CD',
        doador_tipo_ds        = 'Candidato',
        doador_candidatura_id = c.candidatura_id
    from candidatos as c
    where
        receita_origem_sg = 'ROC' 
        and
        doador_tipo_cd = 'PJ'
        and
        doador_cpf_cnpj = c.nr_cnpj_prestador_conta 
;
"""
mtse.execute_query(query_update_doador_id_ROC_PJ)


# ### OUTRO CANDIDATO PESSOA FÍSICA

# In[19]:


query_update_doador_id_ROC_PF = f"""  
with candidatos as
(
select candidato_id, candidatura_id, sg_uf, nr_candidato, nr_cpf_candidato, nr_cnpj_prestador_conta 
from {table_candidaturas}
)
update {table_receitas} as r
    set doador_id             = c.candidato_id,
        doador_tipo_cd        = 'CD',
        doador_tipo_ds        = 'Candidato',
        doador_candidatura_id = c.candidatura_id
    from candidatos as c
    where
        receita_origem_sg = 'ROC' 
        and
        doador_tipo_cd = 'PJ'
        and
        doador_cpf_cnpj = c.nr_cpf_candidato 
;
"""
mtse.execute_query(query_update_doador_id_ROC_PF)


# In[20]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
receita_origem_sg = 'ROC'
and
doador_tipo_cd = 'PJ'""")


# In[21]:


query_update_doador_id_ROC_NUMERO_CARGO = f"""
with candidatos as
(
select candidato_id, candidatura_id, sg_uf, nr_candidato, nr_cpf_candidato, ds_cargo
from {table_candidaturas}
)
update {table_receitas} as r
    set doador_id      = c.candidato_id,
        doador_tipo_cd = 'CD',
        doador_tipo_ds = 'Candidato',
        doador_candidatura_id = c.candidatura_id
    from candidatos as c
    where
        receita_origem_sg = 'ROC' 
        and
        doador_tipo_cd = 'PJ'
        and
        doador_candidato_nr = c.nr_candidato
        --and upper(doador_candidato_cargo_ds) = upper(c.ds_cargo)
        and
        doador_uf = c.sg_uf
"""
mtse.execute_query(query_update_doador_id_ROC_NUMERO_CARGO)


# In[22]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
receita_origem_sg = 'ROC'
and
doador_tipo_cd = 'PJ'""")


# In[23]:


query_update_doador_id_ROC_NOME = f"""
with candidatos as
(
select candidato_id, candidatura_id, sg_uf, nr_candidato, nr_cpf_candidato, nm_candidato, ds_cargo 
from {table_candidaturas}
)
update {table_receitas} as r
    set doador_id      = c.candidato_id,
        doador_tipo_cd = 'CD',
        doador_tipo_ds = 'Candidato',
        doador_candidatura_id = c.candidatura_id
    from candidatos as c
    where
        receita_origem_sg = 'ROC' 
        and
        doador_tipo_cd = 'PJ'
        and
        (
        doador_nome like '%'||c.nm_candidato||'%'
        or
        doador_nome_rfb like '%'||c.nm_candidato||'%'
        )
;
"""
mtse.execute_query(query_update_doador_id_ROC_NOME)


# In[24]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
receita_origem_sg = 'ROC'
and
doador_tipo_cd = 'PJ'""")


# ### ACERTOS ESPECÍFICOS

# ### 2014

# In[25]:


def update_doador_id(nome,id):
    q = f"""
        update {table_receitas} 
            set doador_id      = '{id}',
                doador_tipo_cd = 'CD',
                doador_tipo_ds = 'Candidato',
                doador_candidatura_id = '' 
        where
            {ano_eleicao} = '2014'
            and
            doador_nome like '{nome}'
        ;
        """
    mtse.execute_query(q)

doadores ={
            'BETO RICHA%'                                     :  'CD54191750968',
            'EL 2014 ROGERIO NOGUEIRA LOPES CRUZ DEPUTADO E%' :  'CD13022771894',
            'ELEIÇÃO 2014 MARLENE O DE C MACHADO%'            :  'CD85885177872',
            'ELEIÇÃO 2014 SAGUAS MORAES SOUSA%'               :  'CD28638115172',
            'FERNANDO LESSA LEAO DEPUTADO ESTADUAL%'          :  'CD85642304887',
            'JOSE PEDRO CORDEIRO%'                            :  'CD77795520634',
            'MARIA IZABEL BEZZERA DE SÁ DEPUTADA ESTADUAL%'   :  'CD09296252857',
            'RODRIGO BETHLEM%'                                :  'CD99736870782',
            'RODRIGO ROLLEMBERG%'                             :  'CD24529850153' 
          }


for nome,id in doadores.items():
    update_doador_id(nome,id)


# In[26]:


mtse.execute_query(f"""
                    update {table_receitas}
                    set doador_id = get_doador_id('62227509000129'),
                        doador_cpf_cnpj =  '62227509000129',
                        doador_tipo_cd  =  'PJ'
                    where doador_nome = 'QUANTIQ DISTRIBUIDORA LTDA'
                    and  doador_cpf_cnpj ='00000000000000'
                    and {ano_eleicao} = '2014'
                    ;
                    """
                  )


# In[27]:


mtse.execute_query(f"""
                    update {table_receitas}
                    set doador_id = 'NI',
                        doador_tipo_cd  =  'NI'
                    where doador_nome = 'NÃO IDENTIFICADO'
                    and  doador_cpf_cnpj ='00000000000'
                    and {ano_eleicao} = '2014'
                    ;
                    """
                  )


# ### 2018

# In[28]:


query_update_doador_id_ACERTOS_2018 = f"""
with candidatos as
(
select candidato_id, candidatura_id, sg_uf, nr_candidato, nr_cpf_candidato, nm_candidato, ds_cargo 
from {table_candidaturas}
)
update {table_receitas} as r
    set doador_id      = c.candidato_id,
        doador_tipo_cd = 'CD',
        doador_tipo_ds = 'Candidato',
        doador_candidatura_id = c.candidatura_id
    from candidatos as c
    where
        {ano_eleicao} = '2018'
        and
        receita_origem_sg = 'ROC' 
        and
        doador_tipo_cd = 'PJ'
        and
        (
            c.nm_candidato like '%MARCOS ABRÃO RORIZ SOARES DE CARVALHO%'
            and
            doador_nome like '%MARCOS ABRAO RORIZ S. DE CARVALHO%'
        )
        or
        (
            c.nm_candidato like '%REBECCA MARTINS GARCIA%'
            and
            doador_nome like '%ELEICAO 2018 REBECCA MARTINS GARCIA VICE GOVERNADORA%'      
        )

"""

mtse.execute_query(query_update_doador_id_ACERTOS_2018)


# In[29]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
receita_origem_sg = 'ROC'
and
doador_tipo_cd = 'PJ'""")


# In[30]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
doador_tipo_cd = 'PJ'
""")


# In[31]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
doador_tipo_cd = 'CD'
""")


# ### receitas origem RPP que não são de partido ou candidato (com PJ)

# In[32]:


mtse.pandas_query(f"""
select doador_nome, sum(receita_valor) as valor from {table_receitas}
where
receita_origem_sg in ('RPP')
and doador_tipo_cd = 'PJ'
group by doador_nome
order by valor desc
""")


# In[33]:


mtse.pandas_query(f"""
select doador_nome, doador_nome_rfb, doador_uf, doador_candidato_nr, sum(receita_valor) as valor from {table_receitas}
where
receita_origem_sg in ('ROC')
and doador_tipo_cd = 'PJ'
and upper(doador_nome_rfb) not  like 'ELEI%'
group by doador_nome, doador_nome_rfb, doador_uf, doador_candidato_nr
order by valor desc
""")


# In[ ]:





# ### RECEITA PRÓPRIA

# In[34]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
receita_origem_sg = 'RP'
and
doador_tipo_cd = 'PF'""")


# In[35]:


query_update_doador_id_RP=f"""
    with candidatos as
    (
    select candidato_id,candidatura_id,sg_uf, nr_candidato, nr_cpf_candidato 
    from {table_candidaturas}
    )
    update {table_receitas} as r
        set doador_id                  = c.candidato_id,
            doador_tipo_cd             = 'CA',
            doador_tipo_ds             = 'Candidato',
            doador_candidatura_id      = c.candidatura_id
        from candidatos c
        WHERE
            receita_origem_sg   = 'RP'
            and 
            doador_tipo_cd  = 'PF'
            and 
            doador_cpf_cnpj = c.nr_cpf_candidato             
    ;        
    """

mtse.execute_query(query_update_doador_id_RP)


# ## RECEITA PRÓPRIA

# In[36]:


query_update_doador_id_RP_2=f"""
    update {table_receitas} as r
        set doador_id                  = get_candidato_id(receptor_candidato_cpf),
            doador_tipo_cd             = 'CA',
            doador_tipo_ds             = 'Candidato',
            doador_candidatura_id      = 'CA'||receptor_uf||receptor_candidato_nr
        WHERE
            receita_origem_sg   = 'RP'
            and 
            doador_tipo_cd  = 'PF'           
    ;        
    """

mtse.execute_query(query_update_doador_id_RP_2)


# In[37]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
receita_origem_sg = 'RP'
and
doador_tipo_cd = 'PF'""")


# In[ ]:





# In[38]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
receita_origem_sg = 'RP'
and
doador_tipo_cd = 'PJ'""")


# ### RECEITA DE FINANCIAMENTO COLETIVO

# In[39]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
receita_origem_sg = 'RFC'
and
doador_tipo_cd = 'PJ'""")


# In[40]:


query_update_tipo_doador_id_RFC = f"""
update {table_receitas} as r
    set doador_id = 'RFC'||doador_cpf_cnpj,
        doador_tipo_cd = receita_origem_sg,
        doador_tipo_ds = receita_origem_ds
where
    receita_origem_sg = 'RFC'
    and
    doador_tipo_cd = 'PJ'
;
"""

mtse.execute_query(query_update_tipo_doador_id_RFC)


# ### RECEITA DE APLICAÇÃO FINANCEIRA

# In[41]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
receita_origem_sg = 'RAF'
and
doador_tipo_cd = 'PJ'""")


# In[42]:


query_update_tipo_doador_id_RAF = f"""
update {table_receitas} as r
    set doador_id      = receita_origem_sg,
        doador_tipo_cd = receita_origem_sg,
        doador_tipo_ds = receita_origem_ds,
        doador_cpf_cnpj = '-1',
        doador_nome    = receita_origem_ds
from  {table_origem_receitas} ro
where
        receita_origem_sg = 'RAF'
;
"""

mtse.execute_query(query_update_tipo_doador_id_RAF)


# ### RECEITA DE ORIGEM NÃO IDENTIFICADA

# In[43]:


query_update_tipo_doador_id_RONI = f"""
update {table_receitas} as r
    set doador_id      = receita_origem_sg,
        doador_tipo_cd = receita_origem_sg,
        doador_tipo_ds = receita_origem_ds,
        doador_cpf_cnpj = '-1',
        doador_nome    = receita_origem_ds
from  {table_origem_receitas} ro
where
        receita_origem_sg = 'RONI'
;
"""

mtse.execute_query(query_update_tipo_doador_id_RONI)


# ### RECEITA DE DOADOR NÃO IDENTIFICADO

# In[44]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
doador_tipo_cd = 'PJ'""")


# In[45]:


query_update_tipo_doador_id_NI = f"""
update {table_receitas} as r
    set 
        doador_id      = 'RONI',
        doador_tipo_cd = 'RONI',
        doador_tipo_ds = 'Recursos de origens não identificadas',
        doador_cpf_cnpj = '-1'
from  {table_origem_receitas} ro
where
        doador_id = 'NI'
        or 
        doador_tipo_cd = 'RONI'
;
"""

mtse.execute_query(query_update_tipo_doador_id_NI)


# In[46]:


mtse.pandas_query(f"""
select count(*) from {table_receitas}
where
doador_tipo_cd = 'PJ'""")


# In[47]:


mtse.pandas_query(f"""
    select doador_id,doador_nome, doador_cpf_cnpj, doador_partido_sg, doador_uf, receita_origem_sg, sum(receita_valor) 
    from {table_receitas}
    where
        {ano_eleicao} = '2018'
        and
        doador_tipo_cd = 'PJ'
    group by 
    doador_id,doador_nome, doador_cpf_cnpj, doador_partido_sg, doador_uf, receita_origem_sg
    order by doador_nome, doador_partido_sg, doador_uf, receita_origem_sg
"""
    )


# In[48]:


query_update_doador_candidatura_id=f"""
    with candidatos as
    (
    select candidato_id,candidatura_id 
    from {table_candidaturas}
    )
    update {table_receitas} as r
        set doador_id                  = c.candidato_id,
            doador_tipo_cd             = 'CD',
            doador_tipo_ds             = 'Candidato',
            doador_candidatura_id      = c.candidatura_id
        from candidatos c
        WHERE
            doador_id = c.candidato_id             
    ;        
    """

mtse.execute_query(query_update_doador_candidatura_id)


# In[49]:


import datetime
print(datetime.datetime.now())


# In[ ]:




