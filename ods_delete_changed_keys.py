# Databricks notebook source
from pyspark.sql.functions import from_json, col, explode, max
from delta.tables import *
from datetime import datetime,date,timedelta,timezone

# COMMAND ----------

datalake = "___dataLakeName___"
if "dataLakeName" in datalake:
  datalake = '___'
  
env = "___ambiente___"
if "ambiente" in env:
  env = 'dsv'
  

# COMMAND ----------

# MAGIC %run /general/functions/fnc_retorna_path_containers

# COMMAND ----------

datalake_path = fnc_retorna_path_containers(datalake)

# COMMAND ----------

dbutils.widgets.text("table_name", "")
dbutils.widgets.text("keys", "")
dbutils.widgets.text("ods_database", "")
dbutils.widgets.text("owner", "")
dbutils.widgets.text("produto", "")
dbutils.widgets.text("banco", "")

# COMMAND ----------

table_name = dbutils.widgets.get("table_name")
owner = dbutils.widgets.get("owner")
ods_database = dbutils.widgets.get("ods_database")
produto = dbutils.widgets.get("produto")
banco = dbutils.widgets.get("banco")
keys = dbutils.widgets.get("keys")


# COMMAND ----------

# MAGIC %run /general/functions/fnc_existencia_arquivo

# COMMAND ----------

def get_dfm_full(produto,banco,owner,table_name,datalake_path):
  pathFull=datalake_path['landed']+"attunity/"+produto+"/"+banco+"/"+owner+"."+table_name
  #pegar o ultimo dfm do repositoriaxxx
  path_files_json_dfm_full = sorted(dbutils.fs.ls(pathFull))
  #print(path_files_json_dfm_full)

  if not path_files_json_dfm_full:
    dbutils.notebook.exit("Sem informacaoo")

  #pegar o ultimo dfm do repositorio
  dfm_file_full = path_files_json_dfm_full[-2][0]
  if(".dfm" not in dfm_file_full):
    dbutils.notebook.exit("Sem dfm full para criacao do schema")
  return dfm_file_full

# COMMAND ----------

def get_dfm_incremental(produto,banco,owner,table_name,datalake_path):
  dfm_file = None
  pathIncremental = datalake_path['landed']+"attunity/"+produto+"/"+banco+"/"+owner+"."+table_name+"__ct"
  #pegar a ultima particao do repositorio incremental
  print(pathIncremental)
  path_last_incremental = sorted(dbutils.fs.ls(pathIncremental))
  print(path_last_incremental)
  print(type(path_last_incremental))
  if(len(path_last_incremental)>0):
    path_files_incremental = path_last_incremental[-1][0]
    path_files_json_dfm_incremental = sorted(dbutils.fs.ls(path_files_incremental))
    #print(path_files_json_dfm_incremental)
    if not path_files_json_dfm_incremental:
      #dbutils.notebook.exit("Sem informacao")
      return dfm_file
    #pegar o ultimo dfm do repositorio
    dfm_file_incremental = path_files_json_dfm_incremental[-2][0]
    if(".dfm" in dfm_file_incremental):
      dfm_file = dfm_file_incremental
  return dfm_file

# COMMAND ----------

@udf("int")
def hex_to_int(x):
  
  scale = 16 ## equals to hexadecimal

  return (int(x, scale))

# COMMAND ----------

def remove_changed_keys(incremental_df,keys,datalake_path,produto,banco,owner,table_name):
  
  sub_path = datalake_path['landed']+"attunity/"+produto.lower()+"/"+banco.lower()+"/"
  path = sub_path+""+owner+"."+table_name
  
  dir_list = dbutils.fs.ls(path + "__ct/")
  dir_names = []
  for i in dir_list:
    dir_names.append(i.name[:8])
  
  if not dir_names:
    return incremental_df
  
  dfm_file =get_dfm_incremental(produto,banco,owner,table_name,datalake_path)
  if dfm_file is None:
    dfm_file =get_dfm_full(produto,banco,owner,table_name,datalake_path)
    
    
    
  df_dfm = (spark
                        .read
                        .format("json")
                        .load(dfm_file, multiLine=True)
          )
  
  
  df_dfm_exploded = df_dfm.select(explode(df_dfm.dataInfo.columns))
  
  
  list_dfm_key_ordinals = df_dfm_exploded.select(col('col.ordinal')).where(df_dfm_exploded.col.primaryKeyPos > 0).rdd.flatMap(lambda x: x).collect()
  
  
  
  list_dfm_ordinals = df_dfm_exploded.select(col('col.ordinal')).rdd.flatMap(lambda x: x).collect()
  list_dfm_ordinals.sort()
  max_ordinal = list_dfm_ordinals[-1]
  
  
  df_dfm_colnames = df_dfm_exploded.select(col('col.name'))

  header_offset = df_dfm_colnames.filter(df_dfm_colnames.name.like('header__%')).count()
  header_offset = header_offset-1
  
  
  table_key_mask = '0'*max_ordinal
  table_key_mask = list(table_key_mask[:])

  for i in list_dfm_key_ordinals:
    table_key_mask[i-1] = '1'

  table_key_mask = "".join(table_key_mask[::-1])

  decimal_key_bitwise = int(table_key_mask, 2)
  
  
  
  incremental_df_updates = incremental_df.select("*").where(incremental_df.header__change_mask != "").where(incremental_df.header__change_oper == "U")
  change_mask_df = incremental_df_updates.select("*", hex_to_int("header__change_mask").alias("bitwise_int"))
  
  
  change_mask_df_key_updated = change_mask_df.select('*').where(change_mask_df["bitwise_int"].bitwiseAND(decimal_key_bitwise) > 0)

  if(change_mask_df_key_updated.rdd.isEmpty()):
    return(incremental_df)
  
  
  ta = change_mask_df_key_updated.alias('ta')
  tb = incremental_df.alias('tb')

  tc = ta.join(tb,ta.header__change_seq == tb.header__change_seq).select("tb.*")
  tc = tc.select("*").where(tc.header__change_oper == "B")
  
  tc_tempview_name = "before_images_to_delete"
  tc.createOrReplaceTempView(tc_tempview_name)
  
  incremental_df_tempview_name = "incremental_df"
  incremental_df.createOrReplaceTempView(incremental_df_tempview_name)
  
  where_string = ""
  key_list = keys.split(",")

  incremental_df_alias = 'TGT'
  before_images_to_delete_alias = 'SRC'

  for chave in key_list:
    where_string = where_string + "{0}.{2} = {1}.{2} and ".format(incremental_df_alias,\
                                                                  before_images_to_delete_alias,\
                                                                  chave)


  where_string = where_string[:-5]
  
  
  delete_string = "select * FROM {0} {1} WHERE not EXISTS (Select 1 from {2} {3} where {4})".format(incremental_df_tempview_name,\
                                                                                              incremental_df_alias,\
                                                                                              tc_tempview_name,\
                                                                                              before_images_to_delete_alias,\
                                                                                              where_string )
  
  #spark.sql(delete_string)
  
  return (spark.sql(delete_string))
