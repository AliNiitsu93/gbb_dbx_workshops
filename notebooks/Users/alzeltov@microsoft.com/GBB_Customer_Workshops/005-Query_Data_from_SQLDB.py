# Databricks notebook source
# MAGIC %md # Query Data from Azure SQL DB

# COMMAND ----------

jdbcHostname = "abhidatabricksdemo.database.windows.net"
jdbcDatabase = "abhidatabricksdemo"
jdbcPort = 1433

# COMMAND ----------

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : dbutils.preview.secret.get(scope = "abhi_jdbc", key = "username"),
  "password" : dbutils.preview.secret.get(scope = "abhi_jdbc", key = "password"),
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

housePowerConsAggDf = spark.read.jdbc(url=jdbcUrl, table="house_power_consumption_agg", properties=connectionProperties)

# COMMAND ----------

housePowerConsAggDf.printSchema

# COMMAND ----------

display(housePowerConsAggDf)

# COMMAND ----------

filterQuery = "(select * from dbo.house_power_consumption_agg where MONTH(window_day) = 01 and YEAR(window_day) = 2009) query_alias"
filteredQueryDf = spark.read.jdbc(url=jdbcUrl, table=filterQuery, properties=connectionProperties)

# COMMAND ----------

display(filteredQueryDf)