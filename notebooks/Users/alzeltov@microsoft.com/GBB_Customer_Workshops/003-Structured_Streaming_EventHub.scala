// Databricks notebook source
// MAGIC %md # Structured Streaming with Event Hub

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
spark.conf.set("spark.sql.shuffle.partitions", "10")

// COMMAND ----------

//val namespaceName = "firstnamedatabricksdemo";
val namespaceName = "voyager-adb-eventhub";
val eventHubName = "orders";
val sasKeyName = "RootManageSharedAccessKey";
//val sasKey = ""
val sasKey = dbutils.preview.secret.get(scope = "abhi_eventhub", key = "saskey")

// COMMAND ----------

val eventhubParameters = Map[String, String] (
      "eventhubs.policyname" -> sasKeyName,
      "eventhubs.policykey" -> sasKey,
      "eventhubs.namespace" -> namespaceName,
      "eventhubs.name" -> eventHubName,
      "eventhubs.partition.count" -> "10",
      "eventhubs.consumergroup" -> "consumer_group_1",
      "eventhubs.progressTrackingDir" -> "/tmp/eventhub-orders-progress",
      "eventhubs.maxRate" -> s"1000"
    )

// COMMAND ----------

val schema = StructType(Seq(
  StructField("firstName", StringType, true), 
  StructField("lastName", StringType, true),
  StructField("address", MapType(StringType,StringType) , true),
  StructField("orderValue", StringType, true),
  StructField("orderID", StringType, true),
  StructField("orderTimestamp", TimestampType, true)
))

// COMMAND ----------

val inputStream = spark.readStream.format("eventhubs").options(eventhubParameters)
  .load()

// COMMAND ----------

val df = inputStream.select(from_json('body.cast("string"), schema) as "fields").select($"fields.*")

// COMMAND ----------

display(df)

// COMMAND ----------

val dfSalesByState = df.groupBy($"address.state").agg(sum("orderValue").alias("total_by_state")).orderBy($"total_by_state".desc)

// COMMAND ----------

display(dfSalesByState)

// COMMAND ----------

val dfWrite = df.withColumn("eventDate", $"orderTimestamp".cast("date"))

// COMMAND ----------

// MAGIC %fs mounts

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Please replace **firstname** in *path* and *checkPointLocation* options below

// COMMAND ----------

dfWrite
  .withWatermark("orderTimestamp", "30 seconds")
  .repartition(1)
  .writeStream
  .partitionBy("eventDate")
  .queryName("orders-parquet")
  .format("parquet")
  .option("path", "dbfs:/mnt/abhi-adl/order-events/v1/data")
  .option("checkpointLocation", "dbfs:/mnt/abhi-adl/order-events/v1/checkpoint")
  .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("30 seconds"))
  .start()