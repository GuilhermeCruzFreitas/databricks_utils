# Databricks notebook source
dbutils.widgets.text("data_carga", "")
data_carga = dbutils.widgets.get("data_carga")

dbutils.widgets.text("pipeline_name", "")
pipeline_name = dbutils.widgets.get("pipeline_name")

dbutils.widgets.text("suppress_error", "no")
suppress_error = dbutils.widgets.get("suppress_error")

# COMMAND ----------

from datetime import datetime, timedelta

# COMMAND ----------

now = datetime.strptime(data_carga[:10], "%Y-%m-%d")
d_utc = (now + timedelta(days=1))

lastUpdatedAfter = "{0}T00:00:00.0000000Z".format(now.strftime("%Y-%m-%d"))
lastUpdatedBefore = "{0}T23:59:59.0000000Z".format(d_utc.strftime("%Y-%m-%d"))

# COMMAND ----------

print("data da execução do pipeline alvo:{0}".format(data_carga))
print("nome do pipeline alvo:{0}".format(pipeline_name))

# COMMAND ----------

# MAGIC %run /general/functions/fnc_pegar_status_pipeline_df

# COMMAND ----------

status = get_status(data_carga, pipeline_name)
print(status)

# COMMAND ----------

if not status and suppress_error != "yes":
  raise Exception("Execução da pipeline {0} não encontrada no intervalo de {1} a {2}".format(pipeline_name, lastUpdatedAfter, lastUpdatedBefore))
  
elif not status and suppress_error == "yes":
  dbutils.notebook.exit("Pipeline {0} sem execução na data {1}, porém o fluxo irá continuar devido a supressão de erro estar ativa".format(pipeline_name, data_carga))

# COMMAND ----------

while "InProgress" in status:
  time.sleep(300)
  status = get_status(data_carga, pipeline_name)
  print("verificando novamente")

# COMMAND ----------

if "Succeeded" in status:
  dbutils.notebook.exit("Succeeded")
  
elif "Failed" in status:
  if(suppress_error == "yes"):
    dbutils.notebook.exit("Pipeline {0} executado com erro na data {1}, porém o fluxo irá continuar devido a supressão de erro estar ativa".format(pipeline_name, data_carga))
    
  raise Exception("A execução do pipeline {0} consta como falha em {1}".format(pipeline_name, data_carga))
