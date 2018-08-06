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
# MAGIC * Data is all over the place - reports, KPIs and DS is hard with bigger data from disparate sources on exiting tools
# MAGIC 
# MAGIC ### Why Initech Needs a Data Lake
# MAGIC * Store both big and data in one location for all personas - Data Engineering, Data Science, Analysts 
# MAGIC * They need to access this data in diffrent languages and tools - SQL, Python, Scala, Java, R with Notebooks, IDE, Power BI, Tableau, JDBC/ODBC
# MAGIC 
# MAGIC ### Azure Databricks Solutions
# MAGIC * Azure Storage or Azure Data Lake - Is a place to store all data, big and small
# MAGIC * Access both big (TB to PB) and small data easily with Databricks' scaleable clusters
# MAGIC * Use Python, Scala, R, SQL, Java

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Azure Databricks for Batch ETL & Data Engineers 
# MAGIC 
# MAGIC ![arch](https://kpistoropen.blob.core.windows.net/collateral/roadshow/azure_roadshow_de.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data & Writing Files - Parquet and CSV
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC - Read data from CSV 
# MAGIC - Write out data in optimial Parquet format with Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Reading from CSV

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Data Source
# MAGIC * For this exercise, we will be using a file called **products.csv**.
# MAGIC * The data represents new products we are planning to add to our online store.
# MAGIC * We can use **&percnt;head ...** to view the first few lines of the file.

# COMMAND ----------

# MAGIC %fs ls /mnt/training-sources/initech/productsCsv/

# COMMAND ----------

# MAGIC %fs head /mnt/training-sources/initech/productsCsv/product.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step #1 - Read The CSV File
# MAGIC Let's start with the bare minimum by specifying that the file we want to read is delimited and the location of the file:
# MAGIC The default delimiter for `spark.read.csv( )` is comma but we can change by specifying the option delimiter parameter.

# COMMAND ----------

# A reference to our csv file
csvFile = "/mnt/training-sources/initech/productsCsv/"
tempDF = (spark.read           # The DataFrameReader
    #.option("delimiter", "\t") This is how we could pass in a Tab or other delimiter.
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

display(tempDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step #2 - Infer the Schema
# MAGIC 
# MAGIC Lastly, we can add an option that tells the reader to infer each column's data type (aka the schema)

# COMMAND ----------

df = (spark.read                        # The DataFrameReader
   .option("header", "true")       # Use first line of all files as header
   .option("inferSchema", "true")  # Automatically infer data types
   .csv(csvFile)                   # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step #2 (SQL) - Infer the Schema with SQL

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- DROP TABLE IF EXISTS kp_products; 
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW products
# MAGIC USING CSV
# MAGIC OPTIONS (path "/mnt/training-sources/initech/productsCsv/", header "true", inferSchema "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM products

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2 style="color:green">Do Part 2 of Lab</h2>

# COMMAND ----------

# MAGIC %md Now one might think that this would actually print out the values of the `DataFrame` that we just parallelized, however that's not quite how Apache Spark works. Spark allows two distinct kinds of operations by the user. There are **transformations** and there are **actions**.
# MAGIC 
# MAGIC ### Transformations
# MAGIC 
# MAGIC Transformations are operations that will not be completed at the time you write and execute the code in a cell - they will only get executed once you have called a **action**. An example of a transformation might be to convert an integer into a float or to filter a set of values.
# MAGIC 
# MAGIC ### Actions
# MAGIC 
# MAGIC Actions are commands that are computed by Spark right at the time of their execution. They consist of running all of the previous transformations in order to get back an actual result. An action is composed of one or more jobs which consists of tasks that will be executed by the workers in parallel where possible
# MAGIC 
# MAGIC Here are some simple examples of transformations and actions. Remember, these **are not all** the transformations and actions - this is just a short sample of them. We'll get to why Apache Spark is designed this way shortly!
# MAGIC 
# MAGIC ![transformations and actions](https://training.databricks.com/databricks_guide/gentle_introduction/trans_and_actions.png)

# COMMAND ----------

df.explain(1)

# COMMAND ----------

# MAGIC %md
# MAGIC This can be costly when reading in a large file as spark is forced to read through all the data in the files in order to determine data types.  To read in a file and avoid this costly extra job we can provide the schema to the DataFrameReader.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Reading from CSV w/User-Defined Schema
# MAGIC 
# MAGIC This time we are going to read the same file.
# MAGIC 
# MAGIC The difference here is that we are going to define the schema beforehand to avoid the execution of any extra jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step #1
# MAGIC Declare the schema.
# MAGIC 
# MAGIC This is just a list of field names and data types.

# COMMAND ----------

# Required for StructField, StringType, IntegerType, etc.
from pyspark.sql.types import *

csvSchema = StructType([
  StructField("product_id", LongType(), True),
  StructField("category", StringType(), True),
  StructField("brand", StringType(), True),
  StructField("model", StringType(), True),
  StructField("price", DoubleType(), True),
  StructField("processor", StringType(), True),
  StructField("size", StringType(), True),
  StructField("display", StringType(), True)
 ])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step #2
# MAGIC Read in our data (and print the schema).
# MAGIC 
# MAGIC We can specify the schema, or rather the `StructType`, with the `schema(..)` command:

# COMMAND ----------

productDF = (spark.read                   # The DataFrameReader
  .option('header', 'true')   # Ignore line #1 - it's a header
  .schema(csvSchema)          # Use the specified schema
  .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC With our DataFrame created, we can now create a temporary view and then view the data via SQL:

# COMMAND ----------

display(productDF)

# COMMAND ----------

# create a view called products
productDF.createOrReplaceTempView("products_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM products_df LIMIT 10

# COMMAND ----------

productDF.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from products_df

# COMMAND ----------

# MAGIC %md
# MAGIC And now we can take a peak at the data with simple SQL SELECT statement:

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2 style="color:green">Databricks Tip</h2>
# MAGIC 
# MAGIC * Switch Languages (SQL, Scala, R, Shell, File System)
# MAGIC * JDBC/ODBC!

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM products

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Reading from CSV w/User-Defined Schema with SQL

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS kp_products2;
# MAGIC CREATE TABLE kp_products2 (
# MAGIC   product_id int, 
# MAGIC   category string, 
# MAGIC   brand string, 
# MAGIC   model string, 
# MAGIC   price double,
# MAGIC   processor string,
# MAGIC   size string,
# MAGIC   display string)
# MAGIC USING CSV
# MAGIC OPTIONS (path "/mnt/training-sources/initech/productsCsv/", header "true")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS kp_products2;

# COMMAND ----------

# MAGIC %sql describe formatted kp_products2;

# COMMAND ----------

# MAGIC %sql select * from kp_products2

# COMMAND ----------

df_from_sql = table("kp_products2")

# COMMAND ----------

display(df_from_sql)

# COMMAND ----------

display(df_from_sql.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Writing to Parquet
# MAGIC 
# MAGIC * Parquet is a file format that is supported by many other data processing systems. 
# MAGIC 
# MAGIC * Parquet files are a columnar file format that Databricks highly recommends for customers. 
# MAGIC 
# MAGIC * Parquet files provide optimizations under the hood to speed up queries and are a far more efficient file format than csv or json.
# MAGIC 
# MAGIC * Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema of the original data. When writing Parquet files, all columns are automatically converted to be nullable for compatibility reasons.
# MAGIC 
# MAGIC More discussion on <a href="http://parquet.apache.org/documentation/latest/" target="_blank">Parquet</a>
# MAGIC 
# MAGIC Documentation on <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=dataframe%20reader#pyspark.sql.DataFrameReader" target="_blank">DataFrameReader</a>

# COMMAND ----------

productDF.collect()

# COMMAND ----------

pdf = productDF.toPandas()

# COMMAND ----------

pdf

# COMMAND ----------

productDF.repartition(1).write.mode("overwrite").parquet("dbfs:/tmp/training-msft/initech/Products.parquet")   

# COMMAND ----------

# MAGIC %fs ls /tmp/training-msft/initech/Products.parquet/

# COMMAND ----------

# MAGIC %md we can see the parquet file in the file system

# COMMAND ----------

# MAGIC %fs ls /tmp/training-msft/initech/Products.parquet/

# COMMAND ----------

pq_df = spark.read.parquet('/tmp/training-msft/initech/Products.parquet/')

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Writing to Parquet using SQL

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS kp_products_parquet;
# MAGIC CREATE TABLE kp_products_parquet
# MAGIC USING parquet
# MAGIC OPTIONS (path = "/tmp/sql/training-msft/initech/Products.parquet")
# MAGIC AS 
# MAGIC SELECT * FROM kp_products2

# COMMAND ----------

# MAGIC %fs ls dbfs:/tmp/sql/training-msft/initech/Products.parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kp_products_parquet

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/training-sources/initech/productsCsv/product.csv", "/FileStore/product.csv")
#https://westus.azuredatabricks.net/files/product.csv?o=3076923482971570#

# COMMAND ----------

