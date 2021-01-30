# Databricks notebook source
# MAGIC %md
# MAGIC #### Função para realizar a formatação dos nomes das colunas de um Dataframe (Koalas, Pandas e PySpark). 

# COMMAND ----------

dbutils.library.installPyPI("koalas")

# COMMAND ----------

from pyspark.sql.types import *
import databricks.koalas as ks
import pandas as pd
import re

# Formata os nomes das colunas de um dataframe em Pandas
def fnc_formata_nome_colunas (dataframe):
  columnNames = []
  columnNames = dataframe.columns
  # Define o de-para de caracteres especiais
  special_characters = [['[áãàâäåæ]','[éêèęėēë]','[íîìïįī]','[óõôòöœøō]','[úüùûū]','[ç]','[ñ]','[/\-\n\r.,]','[(){};:º*&^%$#@!+=]'],
                        ['a','e','i','o','u','c','n','_','']]
  amount_of_types = len(special_characters[0])
  
  if isinstance(dataframe, (pd.DataFrame,ks.DataFrame)):  
    for column in columnNames:
      columnName = column
      for index_regex in range(amount_of_types):
        columnName = re.sub(special_characters[0][index_regex], special_characters[1][index_regex], columnName) 
      dataframe.rename(columns={column:columnName}, inplace=True)
    dataframe.columns = dataframe.columns.str.replace(' ', '_')
    dataframe.columns = dataframe.columns.str.lower()
  
  else:
    for column in columnNames:
      columnName = column
      columnName = columnName.lower().replace(' ','_')
      for index_regex in range(amount_of_types):
        columnName = re.sub(special_characters[0][index_regex], special_characters[1][index_regex], columnName) 
      dataframe = dataframe.withColumnRenamed(column,columnName)

  return dataframe
