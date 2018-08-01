// Databricks notebook source
// MAGIC %md # Publish Events to Event Hub

// COMMAND ----------

import org.joda.time
import org.joda.time.format._
import java.sql.Timestamp
import java.nio.ByteBuffer
import scala.util.Random
import com.google.gson.Gson

// COMMAND ----------

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.microsoft.azure.eventhubs._

// COMMAND ----------

//val namespaceName = "firstnamedatabricksdemo";
val namespaceName = "voyager-adb-eventhub";
val eventHubName = "orders";
val sasKeyName = "RootManageSharedAccessKey";
//val sasKey = ""
val sasKey = dbutils.preview.secret.get(scope = "abhi_eventhub", key = "saskey")

// COMMAND ----------

new Timestamp(System.currentTimeMillis());
val dt = new Timestamp(System.currentTimeMillis()-100)

// COMMAND ----------

val recordsPerBatch = 100
val wordsPerRecord = 10
val numBatchesToSend = 1000

// COMMAND ----------

val randomStates = List("CA", "CA", "CA", "MA", "MA", "MA", "RI", "CT", "CT", "NY", "NY", "NY", "NJ", "NJ", "NH")
val randomCites = List("San Fancisco", "San Jose", "Santa Clara", "Boston", "Worcester", "Burlington", "Newport", "Hartford", "New Haven", "New York City", "Buffalo", "Rochester", "Newark", "Jersey City", "Nashua")
val randomFirstName = List("Kyle", "Abhinav", "Tom", "Tony", "Raela", "Keith", "Joe", "Roy", "Vida", "Miklos", "Amy")
val randomLastName = List("Smith", "Harris", "Spark", "Azure")

val eventHubsClient = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);

println(s"Putting records onto stream $eventHubsClient and endpoint $eventHubsClient at a rate of" +
  s" $recordsPerBatch records per batch with $wordsPerRecord words per record for $numBatchesToSend batches")

// Function to generate data

case class Person(firstName: String, lastName: String, address: Address, orderValue: Integer, orderID: Integer, orderTimestamp: String)
case class Address(city: String, state: String)

// COMMAND ----------

def GsonTest() : String = {
    val dt = new Timestamp(System.currentTimeMillis()-100)
    val stateRand = Random.nextInt(randomStates.size)
    val p = Person(randomFirstName(Random.nextInt(randomFirstName.size)),randomLastName(Random.nextInt(randomLastName.size)), Address(randomCites(stateRand), randomStates(stateRand)), scala.util.Random.nextInt(1000), scala.util.Random.nextInt(100000), dt.toString)
    // create a JSON string from the Person, then print it
    val gson = new Gson
    val jsonString = gson.toJson(p)
    return jsonString
}

// COMMAND ----------

 val ehClient = EventHubClient.createFromConnectionStringSync(eventHubsClient.toString());

// COMMAND ----------

// Generate and send the data
for (round <- 1 to numBatchesToSend) {
  for (recordNum <- 1 to recordsPerBatch) {
    val data = GsonTest()
    println(data)
    val sendEvent = new EventData(data.getBytes("UTF-8"));
    ehClient.sendSync(sendEvent);
  }
  Thread.sleep(100) // Sleep for a second
  println(s"Sent $recordsPerBatch records for batch number $round")
}

println("\nAll $numBatchesToSend batches sent")