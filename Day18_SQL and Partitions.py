# Databricks notebook source
jdbc_url = "jdbc:sqlserver://azure-server113.database.windows.net:1433;database=Azure_db"
jdbc_properties={
    "user":"sqllog",
    "password":"sql_Password",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

table_name="saleslt.customer"
df_sq=spark.read.jdbc(url=jdbc_url,table=table_name,properties=jdbc_properties)

# COMMAND ----------

df=spark.read.csv("/mnt/source/student-dataset.csv")

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df = df.repartition(20)  

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df = df.coalesce(5)  

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

from pyspark.storagelevel import *
df.persist(StorageLevel.MEMORY_ONLY)
