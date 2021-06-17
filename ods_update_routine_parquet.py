# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime,date,timedelta,timezone
from pyspark.sql.functions import coalesce
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run /general/ods_tables/functions/ods_delete_changed_keys

# COMMAND ----------

spark = (SparkSession
          .builder
          .appName("update de ods's")
          .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
          .getOrCreate())

# COMMAND ----------

dbutils.widgets.text("table_name", "")
dbutils.widgets.text("keys", "")
dbutils.widgets.text("ods_database", "")
dbutils.widgets.text("owner", "")
dbutils.widgets.text("produto", "")
dbutils.widgets.text("banco", "")
dbutils.widgets.text("data_carga", "")


# COMMAND ----------

table_name = dbutils.widgets.get("table_name")
keys = dbutils.widgets.get("keys")
owner = dbutils.widgets.get("owner")
ods_database = dbutils.widgets.get("ods_database")
produto = dbutils.widgets.get("produto")
banco = dbutils.widgets.get("banco")
data_carga = dbutils.widgets.get("data_carga")

data_carga = data_carga[:10]
data_carga = datetime.strptime(data_carga, "%Y-%m-%d")
d_menos = (data_carga - timedelta(days=1))
data_referencia_partition = ('{}{}{}').format(d_menos.year,d_menos.strftime('%m'), d_menos.strftime('%d'))


essential_columns = ['header__change_oper','_corrupt_record','header__timestamp']
print(table_name)
print(keys)
print(owner)
print(data_referencia_partition)

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

sub_path = datalake_path['raw']+produto.lower()+"/"+banco.lower()+"/"
path = sub_path+""+owner+""+table_name#.upper()

print(path)


# COMMAND ----------

# MAGIC %run /general/functions/fnc_formata_nome_colunas

# COMMAND ----------

# MAGIC %run /general/functions/fnc_existencia_arquivo

# COMMAND ----------

#spark.conf.set("fs.azure.account.key."+datalake+".dfs.core.windows.net",dbutils.secrets.get(scope = "key-vault-"+env,key = "data-lake-key"))

# COMMAND ----------

if fnc_existencia_arquivo(path):
    print("Tabela "+owner+""+table_name+" listada para carga incremental")
else:
     dbutils.notebook.exit("tabela "+owner+""+table_name+" não listada em carga incremental")

# COMMAND ----------

table_list = spark.sql('show tables in ' + ods_database)
table_name_exist = table_list.filter(table_list.tableName == table_name.lower()).collect()

if len(table_name_exist)>0:
    print("Tabela encontrada em " + ods_database + ", começando a atualização...")
else:
    print("Criando "+table_name+" em " + ods_database)
    dbutils.notebook.run(
      "/general/ods_tables/routines/ods_create_routine_parquet",
      timeout_seconds = 6000,
      arguments = {'table_name':table_name,"chaves": keys,'ods_database':ods_database, 'owner':owner,'produto':produto,'banco':banco})
    dbutils.notebook.exit("tabela criada")

# COMMAND ----------

#ods_df = spark.sql("select * from " + ods_database+"."+table_name)
ods_df = DeltaTable.forName(spark, ods_database+"."+table_name)
#desf

# COMMAND ----------

display(ods_df.toDF())

# COMMAND ----------

diretorio_incremental = path + "/"+data_referencia_partition+"/*"
print(diretorio_incremental)

# COMMAND ----------

dir_list = dbutils.fs.ls(path)#(path + "__ct/")

# COMMAND ----------

dir_list

# COMMAND ----------

dir_names = []
for i in dir_list:
  dir_names.append(i.name[:8])

# COMMAND ----------

dir_names

# COMMAND ----------


  
if data_referencia_partition in dir_names:
  incremental_df_with_changed_keys = (spark
    .read
    .option("mergeSchema", "true")
    .format("parquet")
    #.option("primitivesAsString",'true')
    .load(diretorio_incremental))
else:
  dbutils.notebook.exit("sem incremental em: " + diretorio_incremental)

# COMMAND ----------

#removendo registros que tiveram alteração em chave primária para evitar erros de homologação
incremental_df = remove_changed_keys(incremental_df_with_changed_keys,\
                                     keys,\
                                     datalake_path,\
                                    produto,\
                                    banco,\
                                    owner,\
                                    table_name)

# COMMAND ----------

ods_columns = spark.sql("SHOW COLUMNS FROM "+ods_database+"."+table_name)

# COMMAND ----------

display(ods_columns)

# COMMAND ----------

ods_columns_list = columns_full = ods_columns.select(ods_columns["col_name"]).rdd.flatMap(lambda x: x).collect()
type(ods_columns_list)

# COMMAND ----------

#Vendo quais colunas foram dropadas na origem para realizar um "soft drop" na delta, tendo em vista que a delta não permite drop de colunas
columns_to_drop = list(set(map(str.lower,ods_columns_list)) - set(map(str.lower,incremental_df.columns))) 

print(columns_to_drop)

# COMMAND ----------

incremental_df.columns

# COMMAND ----------

incremental_df = fnc_formata_nome_colunas(incremental_df)
incremental_df = incremental_df.select("*").where('header__change_oper in ("D","I","U")')
incremental_df.createOrReplaceTempView('temp_ods_table')
incremental_df = sqlContext.sql("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY "+keys+" ORDER BY header__timestamp desc, header__change_seq desc) AS ROWNUM FROM temp_ods_table)  WHERE ROWNUM = 1")
incremental_df = incremental_df.drop('rownum')

# COMMAND ----------

#display(incremental_df)

# COMMAND ----------

for column_name in columns_to_drop:
  spark.sql('UPDATE '+ods_database+"."+table_name+' SET '+column_name+' = null')

# COMMAND ----------

keys_list = keys.split(",")
keys_list

# COMMAND ----------

ods_alias = 'a'
incremental_alias = 'b'

where_string = ''

for chave in keys_list:
  where_string = where_string + "{0}.{2} = {1}.{2} and ".format(ods_alias,incremental_alias,chave)

where_string = where_string[:-5]

where_string

# COMMAND ----------


ods_df.alias(ods_alias).merge(
    incremental_df.alias(incremental_alias),
    where_string)\
    .whenNotMatchedInsertAll() \
    .whenMatchedUpdateAll() \
    .execute()

# COMMAND ----------

spark.sql("OPTIMIZE "+ods_database+"."+table_name)
spark.sql("VACUUM "+ods_database+"."+table_name+" RETAIN 168 HOURS")
