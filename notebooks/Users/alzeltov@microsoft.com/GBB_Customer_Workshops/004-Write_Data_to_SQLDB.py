# Databricks notebook source
# MAGIC %md # Write Data to Azure SQL DB

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val jdbcUrl = s"jdbc:sqlserver://abhidatabricksdemo.database.windows.net:1433;database=abhidatabricksdemo"

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC 
# MAGIC connectionProperties.put("Driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
# MAGIC connectionProperties.put("user", dbutils.preview.secret.get(scope = "abhi_jdbc", key = "username"))
# MAGIC connectionProperties.put("password", dbutils.preview.secret.get(scope = "abhi_jdbc", key = "password"))

# COMMAND ----------

# MAGIC %sql select * from house_power_consumption_agg

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.table("house_power_consumption_agg").write.jdbc(jdbcUrl, "house_power_consumption_agg", connectionProperties)