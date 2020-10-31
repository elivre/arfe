
# coding: utf-8

# # modulo com funçoes para importação de arquivos (.csv) do TSE e geração de tabelas em banco de dados.

# In[ ]:


dbuser = 'neilor'
dbpassword = 'n1f2c3n1'
dbhost = 'localhost'
dbport = '5432'
dbname = 'getepolitica'


# In[ ]:


unidades_da_federacao={'AC':'ACRE','AL':'ALAGOAS','AM':'AMAZONAS','AP':'AMAPÁ','BA':'BAHIA','BR':'BRASIL','CE':'CEARÁ','DF':'DISTRITO FEDERAL','ES':'ESPÍRITO SANTO','GO':'GOIÁS','MA':'MARANHÃO','MG':'MINAS GERAIS','MS':'MATO GROSSO DO SUL','MT':'MATO GROSSO','PA':'PARÁ','PB':'PARAÍBA','PE':'PERNAMBUCO','PI':'PIAUÍ','PR':'PARANÁ','RJ':'RIO DE JANEIRO','RN':'RIO GRANDE DO NORTE','RO':'RONDÔNIA','RR':'RORAIMA','RS':'RIO GRANDE DO SUL','SC':'SANTA CATARINA','SE':'SERGIPE','SP':'SÃO PAULO','TO':'TOCANTINS'}


# In[ ]:


import os
import sys
import fnmatch
import re
import codecs
import psycopg2 as pg
import pandas as pd
pd.options.display.float_format = '{:,.2f}'.format


# In[ ]:


connect_cmd = f"dbname='{dbname}' user='{dbuser}' host='{dbhost}' password='{dbpassword}'"
postgres_url = f'postgresql://{dbuser}:{dbpassword}@{dbhost}:{dbport}/{dbname}'


# In[ ]:


def open_cursor(query):
    conn=pg.connect(connect_cmd)
    cur = conn.cursor()
    cur.execute(query)
    return cur

def close_cursor(cursor):
    cursor.close()
 

def execute_query(query):
    conn=pg.connect(connect_cmd)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


# In[ ]:

import urllib.request
import zipfile
def import_csv(ano_eleicao,zip_file_url,local_dir):
 
    local_zip_dir = local_dir+'/'+os.path.basename(zip_file_url).split('.')[0]
    if not(os.path.exists(local_dir)):
        os.makedirs(local_dir) 
    os.chdir(local_dir)
    
    zip_file = os.path.basename(zip_file_url)
    zip_file = f"{local_dir}/{zip_file}"
    print(f'Beginning file {zip_file} download ')
    urllib.request.urlretrieve(zip_file_url, zip_file)
    
    z = zipfile.ZipFile(zip_file)
    z.extractall(local_zip_dir)    
       
    os.remove(zip_file)
    
    


# In[ ]:


def csv_header_to_cols(header,sep=';'):
    h=header.lower()
    h1=re.sub(sep,'\t',h)
    h2=re.sub(r'[óôòöõ]','o',h1)
    h3=re.sub(r'[áâãàä]','a',h2)
    h4=re.sub(r'[éêẽèë]','e',h3)
    h5=re.sub(r'[íîĩìï]','i',h4)
    h6=re.sub(r'[úûũùü]','u',h5)
    h7=re.sub(r'[\[\]\{\}\(\)]','',h6)
    h8=re.sub('ç','c',h7)
    h9=re.sub('"','',h8)
    h10=re.sub('\.','',h9)
    h11=re.sub('/','_',h10)
    h12=re.sub(' ','_',h11)
    h13=re.sub(r'[\r\n]','',h12)
    cols = h13.split('\t')
    
    return(cols)



# In[ ]:


def create_table(header_line,table_name,col_sep=';',with_ano_mes=False):
    
    cols=csv_header_to_cols(header_line,col_sep)

    query='create table if not exists '+table_name+'\n(\n'
    
    if with_ano_mes:
        cols_varchar = ['ano varchar','mes varchar']
    else:
        cols_varchar = []
        
    for c in cols:
        cv = c+' varchar'
        cols_varchar.append(cv)
    q2=',\n'.join(cols_varchar)
    query=query+q2+'\n);'
    execute_query(query)          

    


# In[ ]:


def insert_row(cur,table_name,cols,ano=None,mes=None,with_ano_mes=False):
    if with_ano_mes:
        query = "INSERT INTO "+table_name+ '  values ('+"'"+ano+"','"+mes+"',"+cols+');'
    else:
        query = f"INSERT INTO {table_name}  values ({cols});"
    
    cur.execute(query)  

def sanitize_col (col_value):
    value = col_value.replace("'","''")
    value = value.replace('\n','')
    value = value.replace(';',' ')
    value = value.replace('"','')
    value = re.sub(r'#NULO$','#NULO',value)
    value = re.sub(r'#NE$','#NE',value)
    value = value.strip()
    value = "'"+value+"'"
    return value

def load_table(table_name,arq_txt,tem_header,col_sep=';',with_ano_mes=False,ano=None,mes=None,encode='Latin1'):
    conn=pg.connect(connect_cmd)
    cur = conn.cursor()
    n = 0
    with codecs.open(arq_txt,'r',encoding='latin1') as f:
        for line in f:
            n=n+1
            if tem_header:
                header_line=line
                create_table(header_line,table_name,col_sep,with_ano_mes=False)
                
                tem_header = False
            else:
                l10=line.replace(";;",';"#NULO";')
                l11=l10.replace(";;",';"#NULO";')
                l12=l11.replace(";\x00;",';"#NULO";')
                l13=re.sub(r";$",';"#NULO"',l12)
                l14=re.sub(r";\r\n$",';"#NULO',l13)
                
                # values1=l14.split('"'+col_sep+'"')
                values1=l14.split(col_sep)
                
                values1[0]=re.sub('"',"",values1[0])
                values1[-1]=re.sub('"',"",values1[-1])
                values2 = list(map(lambda x : sanitize_col(x), values1)) 

                cols = ','.join(values2) 

                insert_row(cur,table_name,cols,ano,mes,with_ano_mes)


    conn.commit()
    cur.close()
    conn.close


# In[ ]:


def load_arquivos_csv(dbschema,arquivos_dir,tem_header,col_sep):
    for root, dirs, files, rootfd in os.fwalk(arquivos_dir, topdown=False):
        for filename in files:
            file_full_name = arquivos_dir+'/'+filename
            name = filename.split(".")
            if name[1] in ["txt","csv"]:
                fname=name[0].lower()
                uf = fname[-2:].lower()
                if uf.upper() in unidades_da_federacao:
                    table_name = f'{dbschema}.'+re.sub('_'+uf,'',fname)
                    load_table(table_name,file_full_name,tem_header,col_sep)
                else:
                    continue
            else:
                continue


# In[ ]:


def ajusta_valor(table_name,col_name):

    query_zero_valor_nulo = f"""
    update {table_name}
    set {col_name} = '-1'
    where {col_name} like '#NULO' or {col_name} like '';
    """
    execute_query(query_zero_valor_nulo)
    
    query_alter_valor = f"""
    ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE numeric(18,2) 
    USING replace({col_name},',','.')::numeric(18,2);
    """  
    execute_query(query_alter_valor)

def ajusta_valor_inteiro(table_name,col_name):

    query_zero_valor_nulo = f"""
    update {table_name}
    set {col_name} = '0'
    where {col_name} like '#NULO' or {col_name} like '';
    """
    execute_query(query_zero_valor_nulo)
   
    query_alter_valor = f"""
    ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE integer 
    USING replace({col_name},',','.')::integer;
    """  
    execute_query(query_alter_valor)

# In[ ]:


def create_views(table_name,coluna_uf):
    conn=pg.connect(connect_cmd)
    cur = conn.cursor()
    for uf in unidades_da_federacao:
        query = f"create or replace view {table_name}_{uf} as select * from {table_name} where {coluna_uf} like '{uf.upper()}';"
        cur.execute(query)
    conn.commit()
    cur.close()
    conn.close       

def pandas_query(sql_query):
    conn=pg.connect(connect_cmd)
    return pd.read_sql_query(sql_query,conn)
    conn.close