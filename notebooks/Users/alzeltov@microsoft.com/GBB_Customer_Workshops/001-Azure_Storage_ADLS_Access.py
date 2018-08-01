# Databricks notebook source
# MAGIC %md # Azure Storage and ADLS Access

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Download [individual electric power consumption dataset](http://archive.ics.uci.edu/ml/datasets/Individual+household+electric+power+consumption) from UCI ML Repository
# MAGIC * Unzip the file, and extract the .txt
# MAGIC * Use the Data UI to upload the .txt file (but do no create the table)
# MAGIC * Note the full path for uploaded file, and replace in first two cells below

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/household_power_consumption.txt

# COMMAND ----------

# MAGIC %sh head /dbfs/FileStore/tables/household_power_consumption.txt

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([ \
    StructField("Date", StringType(), True), \
    StructField("Time", StringType(), True), \
    StructField("Global_active_power", DoubleType(), True), \
    StructField("Global_reactive_power", DoubleType(), True), \
    StructField("Voltage", DoubleType(), True), \
    StructField("Global_intensity", DoubleType(), True), \
    StructField("Sub_metering_1", DoubleType(), True), \
    StructField("Sub_metering_2", DoubleType(), True), \
    StructField("Sub_metering_3", DoubleType(), True)])

df = (spark.read.csv("dbfs:/FileStore/tables/household_power_consumption.txt", 
                         schema=schema, header=True, 
                         ignoreLeadingWhiteSpace=True, 
                         ignoreTrailingWhiteSpace=True,
                         sep=';'))

# COMMAND ----------

from pyspark.sql import functions as F

df = (
       df.
       withColumn("DateTime", F.to_timestamp(F.concat_ws(' ', df.Date, df.Time), 'dd/MM/yyyy HH:mm:ss')).
       withColumn("Total_metering", (df.Sub_metering_1 + df.Sub_metering_2 + df.Sub_metering_3))
     )

df = df.withColumn("Active_energy", ((df.Global_active_power * (1000/60)) - df.Total_metering))

df_transformed = df.drop("Date", "Time")

display(df_transformed)

# COMMAND ----------

# MAGIC %fs ls /mnt

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS house_power_consumption")
# df_transformed.write.saveAsTable("house_power_consumption", format = "parquet", mode = "overwrite", path = "/mnt/firstname-house-power-consumption")
df_transformed.write.saveAsTable("house_power_consumption", format = "parquet", mode = "overwrite", path = "/mnt/house-power-consumption")

# COMMAND ----------

# MAGIC %sql 
# MAGIC --We can use %sql to query the rows
# MAGIC 
# MAGIC SELECT * FROM house_power_consumption

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT 
# MAGIC   Voltage, Global_active_power 
# MAGIC FROM 
# MAGIC   house_power_consumption

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   to_date(window.start) as window_day, avg_voltage, avg_gbl_active_power, avg_tot_metering
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT  
# MAGIC     window(DateTime, '1 day') as window,
# MAGIC     avg(Voltage) AS avg_voltage,
# MAGIC     avg(Global_active_power) AS avg_gbl_active_power,
# MAGIC     avg(Total_metering) AS avg_tot_metering
# MAGIC   FROM 
# MAGIC     house_power_consumption
# MAGIC   GROUP BY
# MAGIC     window
# MAGIC   ORDER BY
# MAGIC     window.start ASC
# MAGIC )

# COMMAND ----------

outputDf = spark.sql("SELECT to_date(window.start) as window_day, avg_voltage, avg_gbl_active_power, tot_sub_metering_1, tot_sub_metering_2, tot_sub_metering_3, overall_tot_metering FROM (SELECT window(DateTime, '1 day') as window, avg(Voltage) AS avg_voltage, avg(Global_active_power) AS avg_gbl_active_power, sum(Sub_metering_1) AS tot_sub_metering_1, sum(Sub_metering_2) AS tot_sub_metering_2, sum(Sub_metering_3) AS tot_sub_metering_3, sum(Total_metering) AS overall_tot_metering FROM house_power_consumption GROUP BY window ORDER BY window.start ASC)")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS house_power_consumption_agg")
# outputDf.write.saveAsTable("house_power_consumption_agg", format = "parquet", mode = "overwrite", path = "/mnt/firstname-adl/house-power-cons-agg")
outputDf.write.saveAsTable("house_power_consumption_agg", format = "parquet", mode = "overwrite", path = "/mnt/abhi-adl/house-power-cons-agg")

# COMMAND ----------

# MAGIC %sql select * from house_power_consumption_agg