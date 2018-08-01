# Databricks notebook source
# MAGIC %md # Library Installation via Init Scripts

# COMMAND ----------

# MAGIC %md ### 1 - Install custom package

# COMMAND ----------

# MAGIC %md #### Copy the custom package to DBFS - using REST API or CLI

# COMMAND ----------

# MAGIC %fs ls /FileStore/packages

# COMMAND ----------

dbutils.fs.put("dbfs:/databricks/init/rtesting/install-packages.sh","""
#!/bin/bash
R -e "install.packages('/dbfs/FileStore/packages/baselineforecast_0.7.2.gz', repos=NULL, type='source')"

R -e "install.packages('zoo', repos='http://cran.us.r-project.org')"
R -e "install.packages('forecast', repos='http://cran.us.r-project.org')"
""", True)

# COMMAND ----------

# MAGIC %sh cat /dbfs/databricks/init/rtesting/install-packages.sh

# COMMAND ----------

# MAGIC %fs ls /databricks/init/output/rtesting/2018-03-29_14-23-11

# COMMAND ----------

# MAGIC %sh cat /dbfs/databricks/init/output/rtesting/2018-03-29_14-23-11/rtesting-install-packages.sh_10.139.64.16.log

# COMMAND ----------

# MAGIC %md ### 2 - Install Linux Dependencies

# COMMAND ----------

dbutils.fs.put("dbfs:/databricks/init/tm-cluster/install-packages.sh","""
#!/bin/bash
sudo apt-get -y install libgsl-dev
R -e "install.packages('topicmodels', repos='http://cran.us.r-project.org')"
""", True)