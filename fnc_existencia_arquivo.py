# Databricks notebook source
# MAGIC %md
# MAGIC #### Valida se o arquivo ou diretório existe.

# COMMAND ----------

def fnc_existencia_arquivo(path):
  try:
    dbutils.fs.ls(path)
    return 1
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return 0
    else:
      raise
