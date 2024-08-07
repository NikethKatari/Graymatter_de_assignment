# Databricks notebook source
dbutils.widgets.text('First_name',"def")

# COMMAND ----------

First_var=dbutils.widgets.get("First_name")

# COMMAND ----------

Drop_var=dbutils.widgets.get("Drop_down")

# COMMAND ----------

Multi_var=dbutils.widgets.get("Multi_wid")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.fs.ls("/mnt/source")

# COMMAND ----------

df=spark.read.option("header",True).csv("/mnt/source/Custdataact.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

st=StructType().add("CustomerID",StringType(),True)\
            .add("FirstName",StringType(),True)\
            .add("LastName",StringType(),True)\
            .add("Salary",DoubleType(),True)

# COMMAND ----------

df=spark.read.option("header",True).schema(st).csv("/mnt/source/Custdataact.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df_filtered=df.filter(df.FirstName=="Emily")

# COMMAND ----------

df_filtered.display()

# COMMAND ----------

df_filtered.write.format("delta").saveAsTable("manag_cust")

# COMMAND ----------

df_filtered.write.format("delta").save("/mnt/source/Manag_cust/")

# COMMAND ----------

df_fil=df.filter(df.FirstName==First_var)

# COMMAND ----------

df_fil.display()

# COMMAND ----------

df_fil=df.filter(df.FirstName==Drop_var)
df_fil.display()

# COMMAND ----------

df_fil=df.filter(df.LastName==Multi_var)
df_fil.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table Ext_custt
# MAGIC using delta
# MAGIC options (path "abfss://source@adlsgmdegen2.dfs.core.windows.net/Manag_cust")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Ext_custt

# COMMAND ----------

df.createOrReplaceTempView("temp_cust")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_cust where FirstName=='{$Drop_down}'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_cust where LastName in '{$Multi_wid}'

# COMMAND ----------

df_temp=spark.sql(f'''select * from temp_cust where FirstName='{Drop_var}' ''')

# COMMAND ----------

df_temp.display()

# COMMAND ----------

def addStr(s):
    s=s+" " + "bobby"
    return s

# COMMAND ----------

addStr("hey")

# COMMAND ----------

Add_udf=udf(addStr,StringType())

# COMMAND ----------

df_addstr=df.withColumn("addBobby",Add_udf(col("FirstName")))
df_addstr.display()

# COMMAND ----------

spark.udf.register("sql_add",addStr,StringType())

# COMMAND ----------

df.createOrReplaceTempView("temp_Add")

# COMMAND ----------

# MAGIC %sql
# MAGIC select sql_add(FirstName) as namee from temp_Add 

# COMMAND ----------

# MAGIC %run "/Workspace/Users/niketh323@gmail.com/Utility_fold/Utility_Not"

# COMMAND ----------

df_utConcat=df.withColumn("addFunction",concat_udf(col("FirstName")))
df_utConcat.display()

# COMMAND ----------

df_utAdd=df.withColumn("add Ten Thouand",add_udf(col("Salary")))
df_utAdd.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select sql_sub(Salary) as Sub_Salary from temp_Add 

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/niketh323@gmail.com/Utility_fold/Utility_Not",300)

