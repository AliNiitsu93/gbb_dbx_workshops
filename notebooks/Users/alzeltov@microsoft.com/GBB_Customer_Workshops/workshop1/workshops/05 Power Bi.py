# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Challenges
# MAGIC * Larger Data
# MAGIC * Faster data and decisions - seconds, minutes, hours not days or weeks after it is created
# MAGIC * Dashboards and and Reports - for the holiday season, promotional campaigns, track falling or rising trends
# MAGIC 
# MAGIC ### Azure Databricks Solutions
# MAGIC * Larger Data on Azure Blob Storage or Azure Data Lake
# MAGIC * Serverless Clusters
# MAGIC * Secure SQL endpoints for Spark Clusters

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Azure Databricks for Serverless SQL & BI Queries for Data at Scale 
# MAGIC 
# MAGIC ![arch](https://kpistoropen.blob.core.windows.net/collateral/roadshow/azure_roadshow_bi.png)

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %sql cache table kp_products_parquet; 
# MAGIC select * from kp_products_parquet

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from kp_product

# COMMAND ----------

# MAGIC %md 
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)[Azure Databricks & Power BI Integration Docs](https://docs.azuredatabricks.net/user-guide/bi/power-bi.html)