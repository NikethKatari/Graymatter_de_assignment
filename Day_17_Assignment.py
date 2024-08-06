# Databricks notebook source
from pyspark.sql.functions import*
from pyspark.sql.types import *

# COMMAND ----------

dbutils.fs.mount(
  source="wasbs://source@adlsgmdegen2.blob.core.windows.net",
  mount_point="/mnt/source",
  extra_configs={"fs.azure.account.key.adlsgmdegen2.blob.core.windows.net": dbutils.secrets.get(scope="mount_scope", key="New")}
)

# COMMAND ----------

dbutils.fs.ls("/mnt/source")

# COMMAND ----------

df=spark.read.option("header",True).csv("/mnt/source/student-dataset.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.parquet("/mnt/source/parquet/")

# COMMAND ----------

df.write.csv("/mnt/source/csv/")

# COMMAND ----------

df.write.format("delta").saveAsTable("store_asia")

# COMMAND ----------

df.write.format("delta").save("/mnt/source/store_asia/")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ext_us
# MAGIC using delta
# MAGIC options (path "abfss://source@adlsgmdegen2.dfs.core.windows.net/store_asia")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from store_asia
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from store_asia where id=304

# COMMAND ----------

# MAGIC %sql
# MAGIC update store_asia 
# MAGIC set name="niketh"
# MAGIC where id=301

# COMMAND ----------

# MAGIC %sql
# MAGIC describe store_asia

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history store_asia

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended store_asia

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from store_asia  version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table store_asia to version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ext_us values(312,"Conor Paige","United States of America","Sugar Land",	29.62,-95.63,"M","NA",20,2.9,3.1,2.6,5,5,5,6)

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize ext_us

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ext_us values(316,"Steve Rogers","United States of America","Sugar Land",	29.62,-95.63,"M","NA",20,2.9,3.1,2.6,5,5,5,6)

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize ext_us
# MAGIC zorder by (latitude,longitude)
