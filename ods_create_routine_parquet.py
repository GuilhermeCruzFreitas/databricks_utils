# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit
from pyspark.sql.functions import coalesce

# COMMAND ----------

# MAGIC %run /general/functions/fnc_formata_nome_colunas

# COMMAND ----------

# MAGIC %run /general/ods_tables/functions/ods_delete_changed_keys

# COMMAND ----------

# MAGIC %run /general/functions/fnc_existencia_arquivo

# COMMAND ----------

dbutils.widgets.text("table_name", "")
dbutils.widgets.text("chaves", "")
dbutils.widgets.text("ods_database", "")
dbutils.widgets.text("owner", "")
dbutils.widgets.text("produto", "")
dbutils.widgets.text("banco", "")
dbutils.widgets.text("bkp","")

# COMMAND ----------

# MAGIC %md #AREA COMUM

# COMMAND ----------

env = "___ambiente___"
if "ambiente" in env:
  env = 'dsv'
  
datalake = "___dataLakeName___"
if "dataLakeName" in datalake:
  datalake = 'dlsanalyticsdsv'

# COMMAND ----------

# MAGIC %run /general/functions/fnc_retorna_path_containers

# COMMAND ----------

datalake_path = fnc_retorna_path_containers(datalake)

# COMMAND ----------

table_name = dbutils.widgets.get("table_name")
chaves = dbutils.widgets.get("chaves")
owner = dbutils.widgets.get("owner")
ods_db_name = dbutils.widgets.get("ods_database")
produto = dbutils.widgets.get("produto")
banco = dbutils.widgets.get("banco")
bkp = dbutils.widgets.get("bkp")

#essas colunas são essenciais pois o create trabalha com a carga full e a incremental. Quando temos apenas a carga full, não temos o merge das colunas listadas abaixo (pois elas são metadados dos dados incrementais), então, se criassemos a ods
#apenas com as colunas da full, teriamos problemas em alguns passos futuros do passo de update, que fazem calculos baseados nestes metadados
essential_columns = ['header__change_oper','_corrupt_record','header__timestamp','header__change_seq']


print(table_name)
print(chaves)
print(owner)

# COMMAND ----------

sub_path = datalake_path['raw']+produto.lower()+"/"+banco.lower()+"/"
path = sub_path+""+owner+""+table_name#.upper()

print(path)

# COMMAND ----------

table_dir_exists = fnc_existencia_arquivo(path)

# COMMAND ----------

if table_dir_exists:
    print("Tabela "+owner+"."+table_name+" encontrada")
else:
     dbutils.notebook.exit("tabela não encontrada em " + path)

# COMMAND ----------

table_list = spark.sql('show tables in ' + ods_db_name)
table_name_exist = table_list.filter(table_list.tableName == table_name.lower()).collect()

#Verifica se a tabela já tem uma versão ODS, por este ser um script de carga inicial, caso a tabela já exista, a operação será abortada
if len(table_name_exist)>0:
    dbutils.notebook.exit("Tabela encontrada em " + ods_db_name + ", abortando criação da ods table para: "+ table_name)
else:
    print("Criando "+table_name+" em " + ods_db_name)

# COMMAND ----------

display(table_list)

# COMMAND ----------

# MAGIC %md #ÁREA DE PREPARAÇÃO DO MERGE DE FULL COM INCREMENTAIS

# COMMAND ----------

# MAGIC %md ###LENDO O FULL E INCREMENTAIS ATRAVÉS DA NOTAÇÃO DE WILDCARD COM ASTERISCOS

# COMMAND ----------

try:
  df = (spark
    .read
    .option("mergeSchema", "true")
    .format("parquet")
    #.option("primitivesAsString",'true')
    .load(sub_path+""+owner+""+table_name+"/*/*"))#.createOrReplaceTempView("dboFUNDOPAR_INCREMENTAL")"""
except:
  #criar tratamento de erro melhor para este bloco, para o caso de ocorrer um erro acima que não seja a ausencia de arquivos (por exemplo, se houver erro no merge de um schema, hoje será devolvido o erro de dataframe vazio)
  dbutils.notebook.exit("Dataframe vazio")

# COMMAND ----------

df.columns

# COMMAND ----------

for column_name in essential_columns:
  if column_name not in df.columns:
    df = df.withColumn(column_name,lit(None).cast(StringType()))

# COMMAND ----------

#dados provenientes da carga full possuem o header__timestamp nulo, por isso, vamos tratar a data da carga como "a data em que o registro sofreu modificação". Fazemos isso por que na atualização é importante que todos tenham header time stamp
data = df.select(df['data_carga_utc']).where(df['header__timestamp'].isNull())

# COMMAND ----------

data = data.select('*').na.drop(how="all").first()

# COMMAND ----------

data = data[0]

# COMMAND ----------

df = df.drop('_corrupt_record').na.drop(how="all").fillna({ 'header__change_oper':'I'}).fillna({'header__timestamp': data})

#dropando _corrupt_record pois se refere ao arquivo .dfm que contém o schema da tabela origem, será usado no futuro, mas é inutil agora

#transformando 'header__change_oper' null em I para encarar a carga full como 

#preenchendo o null em header__timestamp com a data da carga, para caso haja uma carga full por mudança de schema, a full não seja substituida por um incremental mais antigo 


#display(df)

# COMMAND ----------

#fazendo um dataframe baseado na função que detecta mudanças na chave primaria, isso é importante porque não queremos tratar uma mudança na chave primaria como modificação de registro, já que deste modo, a chave primaria antiga ficaria "morta"
#ocupando espaço na tabela, o que posteriormente poderia dar problemas no count de homologações etc

df_without_changed_keys = remove_changed_keys(df,\
                                     chaves,\
                                     datalake_path,\
                                    produto,\
                                    banco,\
                                    owner,\
                                    table_name)

# COMMAND ----------

#REMOVENDO BEFORE IMAGE E OUTRAS COISAS DESINTERESSANTES
df_final = df_without_changed_keys.select("*").where('header__change_oper in ("D","I","U")')

# COMMAND ----------

#display(df_final)

# COMMAND ----------

df_final.createOrReplaceTempView("temp_ods_table")

# COMMAND ----------

df_temp_ods_table = sqlContext.sql("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY "+chaves+" ORDER BY header__timestamp desc, header__change_seq desc) AS ROWNUM FROM temp_ods_table)  WHERE ROWNUM = 1")

# COMMAND ----------

#display(df_temp_ods_table)

# COMMAND ----------

ods_table = df_temp_ods_table.drop('rownum')

# COMMAND ----------

ods_table = fnc_formata_nome_colunas(ods_table)

# COMMAND ----------

ods_table.write.format('delta').mode("overwrite").saveAsTable(ods_db_name+"."+table_name)

# COMMAND ----------

spark.sql("OPTIMIZE "+ods_db_name+"."+table_name)
spark.sql("VACUUM "+ods_db_name+"."+table_name+" RETAIN 168 HOURS")
