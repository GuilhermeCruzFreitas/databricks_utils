# Databricks notebook source
# MAGIC %md 
# MAGIC ### Função criada para realizar realizar o processo de performance tunning das delta tables 

# COMMAND ----------

def fnc_performing_delta_table(table_name,list_of_partition_columns,list_of_zorder_columns):
  array_of_partition_columns = []
  array_of_partition_columns = list_of_partition_columns.split(',')
  array_of_zorder_columns = []
  array_of_zorder_columns = list_of_zorder_columns.split(',')
  
# Try to show the partition columns of selected table
  try:
    atual_partition_columns = []
    atual_partition_columns = spark.sql('SHOW PARTITIONS ' + table_name).columns

# If the array of provided columns are different of the array of atual partition table columns the function will rebuild the table partition
    if len(array_of_partition_columns) != len(atual_partition_columns):
      df = spark.read.table(table_name)
      (
        df.write
          .format('delta')
          .mode('overwrite')
          .option('overwriteSchema','true')
          .partitionBy(array_of_partition_columns)
          .saveAsTable(table_name)
      )
# Rebuilding the OPTIMIZE command using the provided columns on parameter
      spark.sql('OPTIMIZE ' + table_name + ' ZORDER BY ('+ ','.join(array_of_zorder_columns) +')')
  
# Vacuum table files that will not be used anymore
      spark.sql('VACUUM ' + table_name + ' RETAIN 168 HOURS')
    else:  

# If the array of provided columns are composed for only one column and this column are the same used for the atual partition table do nothing
      if (len(array_of_partition_columns) == 1) and (array_of_partition_columns in atual_partition_columns):

# Rebuilding the OPTIMIZE command using the provided columns on parameter
        spark.sql('OPTIMIZE ' + table_name + ' ZORDER BY ('+ ','.join(array_of_zorder_columns) +')')
  
# Vacuum table files that will not be used anymore
        spark.sql('VACUUM ' + table_name + ' RETAIN 168 HOURS')
      else:

# If the array of provided columns are composed for more than one column check if all provided columns exists on the atual array of partitions columns
        columns_amount = 0
        for column in atual_partition_columns:
          if column in array_of_partition_columns:
            columns_amount += 1
          else:
            pass
  
# If the amount of provided columns that exists on the atual array of partitions columns are not the same number the function will rebuild the table partition
        if columns_amount != len(array_of_partition_columns):
          df = spark.read.table(table_name)
          (
            df.write
              .format('delta')
              .mode('overwrite')
              .option('overwriteSchema','true')
              .partitionBy(array_of_partition_columns)
              .saveAsTable(table_name)
          )
  
# Rebuilding the OPTIMIZE command using the provided columns on parameter
          spark.sql('OPTIMIZE ' + table_name + ' ZORDER BY ('+ ','.join(array_of_zorder_columns) +')')
  
# Vacuum table files that will not be used anymore
          spark.sql('VACUUM ' + table_name + ' RETAIN 168 HOURS')
        else:
# Rebuilding the OPTIMIZE command using the provided columns on parameter
          spark.sql('OPTIMIZE ' + table_name + ' ZORDER BY ('+ ','.join(array_of_zorder_columns) +')')
# Vacuum table files that will not be used anymore
          spark.sql('VACUUM ' + table_name + ' RETAIN 168 HOURS')
    print('A tabela ' + table_name + ' está particionada pela(s) coluna(s): ' + ','.join(array_of_partition_columns) + '.')
  
# If the column are not partitioned the error handle will build the table partition using the parameter
  except Exception as e:
    if 'SHOW PARTITIONS is not allowed on a table that is not partitioned' in str(e):
      df = spark.read.table(table_name)
      (
        df.write
          .format('delta')
          .mode('overwrite')
          .option('overwriteSchema','true')
          .partitionBy(array_of_partition_columns)
          .saveAsTable(table_name)
      )
# Building the OPTIMIZE command using the provided columns on parameter
      spark.sql('OPTIMIZE ' + table_name + ' ZORDER BY ('+ ','.join(array_of_zorder_columns) +')')
  
# Vacuum table files that will not be used anymore
      spark.sql('VACUUM ' + table_name + ' RETAIN 168 HOURS')
      print('A tabela ' + table_name + ' foi particionada pela(s) coluna(s): ' + ','.join(array_of_partition_columns) + '.')
    else:
      raise
