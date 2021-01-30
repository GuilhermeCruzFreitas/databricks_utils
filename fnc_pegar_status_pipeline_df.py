# Databricks notebook source
# MAGIC %md #Função responsável pelo retorno do status atualizado da última execução de um pipeline da data especificada.
# MAGIC 
# MAGIC Parâmetros:
# MAGIC 1. data_carga: data desejada da consulta 
# MAGIC 2. pipeline_name: nome do pipeline a ser consultado
# MAGIC 
# MAGIC Possíveis retornos: **Sucedded**, **InProgress**, **Failed** e vazio.

# COMMAND ----------

dbutils.library.installPyPI("azure-mgmt-datafactory")
dbutils.library.installPyPI("azure-mgmt-resource")
dbutils.library.installPyPI("azure.identity")

# COMMAND ----------

##Mudanca para utilizar ClientSecretCredential 
##from azure.common.credentials import ServicePrincipalCredentials
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory import *
from datetime import datetime, timedelta 
import time
from pyspark.sql.functions import col,first, max as max_

# COMMAND ----------

def get_status(data_carga, pipeline_name):
  
  env = "___ambiente___"
  tenant = "___tenantIdentity___"
  df_name = "___dataFactoryName___"
  rg_name = "___resourceGroupName___"
  client_id = "___clientIdDatabricksSpKey___"
  subscription_id = "___subscriptionId___"

  if "ambiente" in env:
    env = 'dsv'
    tenant = '___'
    df_name = '___'
    rg_name = '___'
    client_id = '___'
    #secret = 'EoG5xI8_k1J?yEbj'
    subscription_id = '___'

  secret = dbutils.secrets.get(scope = "key-vault-"+env,key = "key_name")
  
  credentials = ClientSecretCredential(tenant_id=tenant,client_id=client_id, client_secret=secret)
  resource_client = ResourceManagementClient(credentials, subscription_id)
  adf_client = DataFactoryManagementClient(credentials, subscription_id)
  
  now = datetime.strptime(data_carga[:10], "%Y-%m-%d")
  d_utc = (now + timedelta(days=1))

  lastUpdatedAfter = "{0}T03:00:00.0000000Z".format(now.strftime("%Y-%m-%d"))
  lastUpdatedBefore = "{0}T02:59:59.0000000Z".format(d_utc.strftime("%Y-%m-%d"))
  
  filter_parameters = {"lastUpdatedAfter": lastUpdatedAfter,
                         "lastUpdatedBefore": lastUpdatedBefore}
  
  pipeline_runs = adf_client.pipeline_runs.query_by_factory(rg_name, df_name, filter_parameters)
  
  query_result = sc.parallelize(pipeline_runs.as_dict()['value']).toDF(sampleRatio=0.2)
  
  pipeline_last_run = query_result.withColumn("run_start", col("run_start").cast("timestamp")).where("pipeline_name = '" + pipeline_name+"'").groupBy().agg(max_("run_start"))
  
  pipeline_last_run = pipeline_last_run.withColumn('run_start',col("max(run_start)"))
  
  status = query_result.join(pipeline_last_run, query_result.run_start == pipeline_last_run.run_start).select("status").rdd.flatMap(lambda x: x).collect()
  
  return status

# COMMAND ----------


