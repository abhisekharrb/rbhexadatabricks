# Databricks notebook source
df_drivers=spark.read.json("dbfs:/mnt/asadlsad/processeddata/raw/drivers.json")
df_pitstop=spark.read.option("multiline",True).json("dbfs:/mnt/asadlsad/processeddata/raw/pit_stops.json")

# COMMAND ----------

df_drivers.join(df_pitstop).display()

# COMMAND ----------

df_drivers.join(df_pitstop,"driverId").display()

# COMMAND ----------

df_drivers.join(df_pitstop,"driverId","left").display()

# COMMAND ----------


df_drivers.join(df_pitstop,"driverId","left").select("driverId","code","lap").display()

# COMMAND ----------


