# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_01=spark.read.csv("/FileStore/gmde/googleplaystore__1_.csv")

# COMMAND ----------

df_01.show()

# COMMAND ----------

df_01.display()

# COMMAND ----------

df_01.printSchema()

# COMMAND ----------

df_01=spark.read.option("header",True).csv("/FileStore/gmde/googleplaystore__1_.csv")

# COMMAND ----------

df_01.printSchema()

# COMMAND ----------

df_02=spark.read.option("header",True).option("inferschema",True).csv("/FileStore/gmde/googleplaystore__1_.csv")

# COMMAND ----------

df_02.printSchema()

# COMMAND ----------

st=StructType().add("App",StringType(),True)\
            .add("Category",StringType(),True)\
            .add("Rating",DoubleType(),True)\
            .add("Reviews",IntegerType(),True)\
            .add("Size",StringType(),True)\
            .add("Installs",StringType(),True)\
            .add("Type",StringType(),True)\
            .add("Price",StringType(),True)\
            .add("Content Rating",StringType(),True)\
            .add("Genres",StringType(),True)\
            .add("Last Updated",StringType(),True)\
            .add("Current Ver",StringType(),True)\
            .add("Android Ver",StringType(),True)
            
             

# COMMAND ----------

df_03=spark.read.option("header",True).schema(st).csv("/FileStore/gmde/googleplaystore__1_.csv")

# COMMAND ----------

df_03.display()

# COMMAND ----------

df_03.printSchema()

# COMMAND ----------

df_04=df_03.withColumn("Multi",col("Rating")*col("Reviews"))

# COMMAND ----------

df_04.printSchema()

# COMMAND ----------

df_04.display()

# COMMAND ----------

df_05=df_03.withColumn("App_again",col("App"))

# COMMAND ----------

df_05.printSchema()

# COMMAND ----------

df_06=df_03.withColumn("owner",lit("Niketh"))

# COMMAND ----------

df_06.printSchema

# COMMAND ----------

df_06.display()

# COMMAND ----------

df_07=df_03.withColumn("Multi",col("Rating")*col("Reviews")).withColumn("App_again",col("App")).withColumn("owner",lit("Niketh"))

# COMMAND ----------

df_07.printSchema()

# COMMAND ----------

df_08=df_03.withColumnRenamed("Installs","NewInsta")

# COMMAND ----------

df_08.printSchema()

# COMMAND ----------

df_09=df_03.select("App","Rating")

# COMMAND ----------

df_10=df_03.selectExpr('cast(Rating as integer) as new_rating')

# COMMAND ----------

df_11=df_03.sort(col("Current Ver").desc())

# COMMAND ----------

df_11.display()

# COMMAND ----------

df_01.count()

# COMMAND ----------

df_12=df_03.distinct()
df_12.display()

# COMMAND ----------

df_13=df_03.dropDuplicates(["Category","Rating"])

# COMMAND ----------

df_13.display()

# COMMAND ----------

casee=df_12.withColumn("Rating",when((col("Rating")>4.3) & (col("Rating")<=4.5),"Average").when((col("Rating")>4.5) & (col("Rating")<=4.8),"Good").when(col("Rating")>4.8,"Excellent").otherwise(col("Rating")))
        
        

# COMMAND ----------

casee.display()

# COMMAND ----------

df_newres=spark.read.csv("/FileStore/gmde/googleplaystore_user_reviews.csv")

# COMMAND ----------


stt=StructType().add("App",StringType(),True)\
            .add("Translated_Review",StringType(),True)\
            .add("Sentiment",StringType(),True)\
            .add("Sentiment_Polarity",DoubleType(),True)\
            .add("Sentiment_Subjectivity",DoubleType(),True)
            

# COMMAND ----------

df_newresss=spark.read.option("header",True).schema(stt).csv("/FileStore/gmde/googleplaystore_user_reviews.csv")

# COMMAND ----------

df_sent=df_03.join(df_newresss,df_03.App==df_newresss.App,how="left").select(df_03['*'],df_newresss["Sentiment"])

# COMMAND ----------

df_sent.printSchema()

# COMMAND ----------

df_agg=df_sent.groupBy("App").avg("Rating")

# COMMAND ----------

df_agg.display()
