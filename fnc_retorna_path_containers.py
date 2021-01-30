# Databricks notebook source
# MAGIC %md
# MAGIC #### Função utilizada para retornar o path para os containers de acordo com o nome do cluster.

# COMMAND ----------

def fnc_retorna_path_containers(datalake):
  datalake_path = {}
  # Essa variável precisa ser alterada caso um novo container seja adicionado 
  containers = [
    'raw',
    'silver',
    'gold',
    'file-store',
    'landed'
  ]
  
  if 'sandbox' in spark.conf.get("spark.databricks.clusterUsageTags.clusterName"):
    for container in containers:
      datalake_path[container] = "/mnt/{}/".format(container)
  else:
    for container in containers:
      datalake_path[container] = "abfss://{}@{}.dfs.core.windows.net/".format(container,datalake)
  return datalake_path
