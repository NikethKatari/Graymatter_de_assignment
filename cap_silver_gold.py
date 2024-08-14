# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

df_sliver_Patients=spark.read.parquet("/mnt/input/HealthCareProject/Cap_Silver/Patients")

# COMMAND ----------

df_sliver_Patients.display()

# COMMAND ----------

df_sliver_Treatment=spark.read.parquet("/mnt/input/HealthCareProject/Cap_Silver/Treatment")

# COMMAND ----------

df_sliver_Treatment.display()

# COMMAND ----------

df_sliver_Hospital=spark.read.parquet("/mnt/input/HealthCareProject/Cap_Silver/Hospital")

# COMMAND ----------

df_sliver_Hospital.display()

# COMMAND ----------

df_sliver_staffs=spark.read.parquet("/mnt/input/HealthCareProject/Cap_Silver/Staffs")

# COMMAND ----------

df_sliver_staffs.display()

# COMMAND ----------

df_sliver_visit=spark.read.parquet("/mnt/input/HealthCareProject/Cap_Silver/Visits")

# COMMAND ----------

df_sliver_visit.display()

# COMMAND ----------

df_sliver_outcomes=spark.read.parquet("/mnt/input/HealthCareProject/Cap_Silver/Outcomes")

# COMMAND ----------

df_sliver_outcomes.display()

# COMMAND ----------

df_Patients_Treatment = df_sliver_Patients.join(df_sliver_Treatment,df_sliver_Patients.PatientID == df_sliver_Treatment.PatientID).drop(df_sliver_Treatment.PatientID)

# COMMAND ----------

df_Patients_Treatment.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Total treatment costs per patient

# COMMAND ----------

df_treatment_costs = df_Patients_Treatment.groupBy('PatientID', 'Name') \
    .agg(sum('Cost').alias('TotalTreatmentCost'))

# COMMAND ----------

df_treatment_costs.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Average Length Of Hospital Stay

# COMMAND ----------

df_length_of_stay = df_sliver_Patients.withColumn(
    "LengthOfStay",
    datediff(col("formatted_date"), col("StartDate"))
)

# Calculate the average length of stay
average_length_of_stay_df =df_length_of_stay.select("LengthOfStay") \
    .agg(avg("LengthOfStay").alias("AverageLengthOfStay"))

# COMMAND ----------

df_length_of_stay.display()

# COMMAND ----------

average_length_of_stay_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Frequency of Visist By Diagnosis

# COMMAND ----------

df_Patients_Treatment_visit = df_Patients_Treatment.join(df_sliver_visit,df_Patients_Treatment.TreatmentID == df_sliver_visit.TreatmentID).drop(df_sliver_visit.TreatmentID)

# COMMAND ----------

df_Patients_Treatment_visit.display()

# COMMAND ----------

df_visit_Frequency =  df_Patients_Treatment_visit.groupBy("Diagnosis").agg(count("visitID").alias("visitFrequency"))

# COMMAND ----------

df_visit_Frequency.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Staffing levels

# COMMAND ----------

df_Hospital_staff = df_sliver_Hospital.join(df_sliver_staffs,df_sliver_Hospital.HospitalID == df_sliver_staffs.HospitalID).drop(df_sliver_staffs.HospitalID)

# COMMAND ----------

df_Hospital_staff.display()

# COMMAND ----------

df_staff_level = df_Hospital_staff.groupBy("HospitalName").agg(count("Role").alias("RoleCount"))

# COMMAND ----------

df_staff_level.display()

# COMMAND ----------

df_ex=df_Hospital_staff.groupBy("Role","HospitalName").count()
df_ex.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Business use case 1
# MAGIC #### Cost Management

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

treatment_costs = df_sliver_Treatment.groupBy("TreatmentType") \
    .agg(
        avg("Cost").alias("AvgCost"),
        sum("Cost").alias("TotalCost")
    ) \
    .orderBy(desc("TotalCost"))


# COMMAND ----------

treatment_costs.display()

# COMMAND ----------

joined_df = df_sliver_Treatment.join(df_sliver_visit, on="TreatmentID", how="inner")
joined_df = joined_df.join(df_sliver_staffs, on="staffID", how="inner")
final_df = joined_df.join(df_sliver_Hospital, on="HospitalID", how="inner")
final_df.display()

# COMMAND ----------

df_costrequirement=final_df.groupBy("HospitalName","TreatmentType").agg(sum("Cost").alias("TreatmentCost")).orderBy("TreatmentCost", ascending=False)
df_costrequirement.display()

# COMMAND ----------

df_costrequirement.write.mode("overwrite").format("delta").saveAsTable("requirementOne")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from requirementOne

# COMMAND ----------

# MAGIC %sql
# MAGIC Select HospitalName,TreatmentType,min(TreatmentCost) from requirementOne where TreatmentType = 'Surgery' group by HospitalName,TreatmentType

# COMMAND ----------

dfpp= df_costrequirement.groupBy("TreatmentType").agg(min("TreatmentCost").alias("TreatmentCost"))

# COMMAND ----------

dfpp.display()

# COMMAND ----------

df_treatment_cost_requirement=df_costrequirement.join(dfpp,on=["TreatmentType","TreatmentCost"])
df_treatment_cost_requirement.select("HospitalName","TreatmentType","TreatmentCost").display()

# COMMAND ----------

df_staff_hospital=df_sliver_staffs.join(df_sliver_Hospital,df_sliver_staffs.HospitalID == df_sliver_Hospital.HospitalID).drop(df_sliver_Hospital.HospitalID)
df_staff_hospital.display()

# COMMAND ----------

df_grouped = df_Hospital_staff.groupBy("HospitalName").agg(
    count("Role").alias("TotalStaffLevels"),
    count(when(col("status") == 1, True)).alias("ActiveStaffLevels")
)

df_resultrole = df_grouped.withColumn("ActiveStaffPercentage", (col("ActiveStafflevels") / col("TotalStaffLevels")) * 100)

df_resultrole.select("HospitalName", "ActiveStaffPercentage").display()

# COMMAND ----------

df_grouped.display()

# COMMAND ----------


