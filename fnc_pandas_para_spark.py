# Databricks notebook source
# MAGIC %md
# MAGIC #### Função para transformar pandas_df em PySpark_df. 

# COMMAND ----------

def equivalent_type(datatype):
  choices = {
    'datetime64[ns]': TimestampType(), 
    'int64': LongType(),
    'int32': IntegerType(),
    'float64': FloatType()
  }
  return choices.get(datatype, StringType())

def define_structure(string, format_type):
  try: typo = equivalent_type(format_type)
  except: typo = StringType()
  return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def fnc_pandas_para_spark(pandas_df):
  columns = list(pandas_df.columns)
  types = list(pandas_df.dtypes)
  struct_list = []
  for column, typo in zip(columns, types): 
    struct_list.append(define_structure(column, str(typo)))
    p_schema = StructType(struct_list)
  return spark.createDataFrame(pandas_df, p_schema)
