# Databricks notebook source
# MAGIC %md
# MAGIC #### Função para retirar caracteres especiais da string. 

# COMMAND ----------

import re 

def fnc_remove_especial_char(string):
  special_characters = [  ['[áãàâäåæ]','[ÁÃÀÂÄÅÆ]','[éêèęėēë]','[ÉÊÈĘĖĒË]','[íîìïįī]','[ÍÎÌÏĮĪ]','[óõôòöœøō]','[ÓÕÔÒÖŒØŌ]','[úüùûū]','[ÚÜÙÛŪ]','[ç]','[Ç]','[ñ]','[Ñ]'],
    ['a','A','e','E','i','I','o','O','u','U','c','C','n','N']
  ]
  
  for index in range(len(special_characters[0])):
    string = re.sub(special_characters[0][index], special_characters[1][index], string)
  return string
