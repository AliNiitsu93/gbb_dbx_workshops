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
# MAGIC * Business wants better product recommendations and conversion on website and emails
# MAGIC * Data Science spends most of their time connecting and wrangling data, very little on actual data science
# MAGIC * Data Science is hard to scale from sample data to large data sets
# MAGIC 
# MAGIC 
# MAGIC ### Azure Databricks Solutions
# MAGIC * With all the data in one place (Azure Storage, Azure Data Lake), Easy for DS to spend time on DS
# MAGIC * Azure Databricks Scales to ML on GB, TB, PB of Data
# MAGIC * Easily go into production with ML (save results to CosmosDB)
# MAGIC 
# MAGIC ### Why Initech uses Azure Databricks for ML
# MAGIC * Millions of users and 100,000s of prodcuts, product reccomendations need more than a single machine
# MAGIC * Easy APIs for newer data science team
# MAGIC * Store results in CosmosDB for online serving (emails, website, etc)

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Azure Databricks for Machine Learning and Data Scientists
# MAGIC ![arch](https://kpistoropen.blob.core.windows.net/collateral/roadshow/azure_roadshow_ml.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC # ![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Providing Product Recommendations
# MAGIC 
# MAGIC One of the most common uses of big data is to predict what users want.  This allows Google to show you relevant ads, Amazon to recommend relevant products, and Netflix to recommend movies that you might like.  This lab will demonstrate how we can use Apache Spark to recommend products to a user.  
# MAGIC 
# MAGIC We will start with some basic techniques, and then use the SparkML library's Alternating Least Squares method to make more sophisticated predictions. Here are the SparkML [Python docs](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html) and the [Scala docs](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.package).
# MAGIC 
# MAGIC For this lesson, we will use around 900,000 historical product ratings from our company Initech.
# MAGIC 
# MAGIC In this lab:
# MAGIC * *Part 0*: Exploratory Analysis
# MAGIC * *Part 1*: Collaborative Filtering
# MAGIC * *Part 2*: Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) *Part 0:* Exploratory Analysis
# MAGIC 
# MAGIC Let's start by taking a look at our data.  It's already mounted in `/mnt/training-msft/ratings.parquet` table for us.  Exploratory analysis should answer questions such as:
# MAGIC 
# MAGIC * How many observations do I have?
# MAGIC * What are the features?
# MAGIC * Do I have missing values?
# MAGIC * What do summary statistics (e.g. mean and variance) tell me about my data?
# MAGIC 
# MAGIC Start by importing the data.  Bind it to `productRatings` by running the cell below

# COMMAND ----------

product_ratings = spark.read.parquet("dbfs:/mnt/training-sources/initech/productRatings/")

# COMMAND ----------

display(product_ratings)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a count of the data using the `count()` DataFrame method.

# COMMAND ----------

#TO-DO
<<FILL-IN>>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Let's look at what these product_ids mean?
# MAGIC 
# MAGIC * There is a product lookup dataset in parquet located here: `dbfs:/mnt/training-sources/initech/productsShort/`

# COMMAND ----------

#TO-DO
product_df = <<FILL-IN>> #use the Spark parquet reader like the productRatings DataFrame above to read the products data

# COMMAND ----------

display(product_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) *Part 1:* Collaborative Filtering
# MAGIC 
# MAGIC The image below (from [Wikipedia][collab]) shows an example of predicting of the user's rating using collaborative filtering. At first, people rate different items (like videos, products, articles, images, games). After that, the system is making predictions about a user's rating for an item, which the user has not rated yet. These predictions are built upon the existing ratings of other users, who have similar ratings with the active user. For instance, in the image below the system has made a prediction, that the active user will not like the video.  
# MAGIC ![collaborative filtering](https://courses.edx.org/c4x/BerkeleyX/CS100.1x/asset/Collaborative_filtering.gif)
# MAGIC 
# MAGIC [SparkML]: http://spark.apache.org/docs/latest/ml-guide.html
# MAGIC [collab]: https://en.wikipedia.org/?title=Collaborative_filtering
# MAGIC [collab2]: http://recommender-systems.org/collaborative-filtering/

# COMMAND ----------

 #We'll hold out 60% for training, 20% of our data for validation, and leave 20% for testing 
seed = 1800009193L
(training_df, validation_df, test_df) = product_ratings.randomSplit([.6, .2, .2], seed=seed)

# COMMAND ----------

display(training_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### My Ratings
# MAGIC * Fill in your ratings for the above `product_df`
# MAGIC * Pick 5-10 product ids to rate
# MAGIC * Choose your ratings be 1-5

# COMMAND ----------

#TO-DO
my_user_id = 0
my_rated_products = [
     (1, my_user_id, <<FILL-IN>>), # Replace with your ratings.
      <<FILL-IN>>
     ]

# COMMAND ----------

my_ratings_df = spark.createDataFrame(my_rated_products, ['product_id','user_id','rating'])

# COMMAND ----------

# MAGIC %md Join your ratings with the `product_df` to see your ratings with the product metadata

# COMMAND ----------

display(my_ratings_df.join(product_df, ['product_id']))

# COMMAND ----------

# MAGIC %md Union your ratings with the `trainingDF` to see your ratings with the product metadata

# COMMAND ----------

training_with_my_ratings_DF = training_df.union(my_ratings_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Alternating Least Squares
# MAGIC 
# MAGIC In this part, we will use the Apache Spark ML Pipeline implementation of Alternating Least Squares, [ALS (Python)](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.recommendation.ALS) or [ALS (Scala)](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.recommendation.ALS). ALS takes a training dataset (DataFrame) and several parameters that control the model creation process.
# MAGIC 
# MAGIC The process we will use for determining the best model is as follows:
# MAGIC 1. Pick a set of model parameters. The most important parameter to model is the *rank*, which is the number of columns in the Users matrix (green in the diagram above) or the number of rows in the Products matrix (blue in the diagram above). In general, a lower rank will mean higher error on the training dataset, but a high rank may lead to [overfitting](https://en.wikipedia.org/wiki/Overfitting).  We will train models with a rank of 2 using the `trainingDF` dataset.
# MAGIC 
# MAGIC 2. Set the appropriate parameters on the `ALS` object:
# MAGIC     * The "User" column will be set to the values in our `user_id` DataFrame column.
# MAGIC     * The "Item" column will be set to the values in our `product_id` DataFrame column.
# MAGIC     * The "Rating" column will be set to the values in our `rating` DataFrame column.
# MAGIC     * We'll be using a regularization parameter of 0.1.
# MAGIC     
# MAGIC    **Note**: Read the documentation for the ALS class **carefully**. It will help you accomplish this step.
# MAGIC 3. Have the ALS output transformation (i.e., the result of `ALS.fit()`) produce a _new_ column
# MAGIC    called "prediction" that contains the predicted value.
# MAGIC 
# MAGIC 4. Create multiple models using `ALS.fit()`, one for each of our rank values. We'll fit 
# MAGIC    against the training data set (`trainingDF`).
# MAGIC 
# MAGIC 5. We'll run our prediction against our validation data set (`validationDF`) and check the error.
# MAGIC 
# MAGIC 6. Use `.setColdStartStrategy("drop")` so that the model can deal with missing values.

# COMMAND ----------

from pyspark.ml.recommendation import ALS

# Let's initialize our ALS learner
als = ALS()

# Now we set the parameters for the method
(als.setPredictionCol("prediction")
   .setUserCol("user_id")
   .setItemCol("product_id")
   .setRatingCol("rating")
   .setMaxIter(5)
   .setSeed(seed)
   .setRegParam(0.1)
   .setRank(2)
   .setColdStartStrategy("drop")
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Validation: 
# MAGIC Let's see how we did against know ratings

# COMMAND ----------

#TO-DO
model = als.fit(<<FILL-IN>>) #fill in with training_with_my_ratings_DF
# Run the model to create a prediction. Predict against the validationDF.
predict_df = model.transform(validation_df)

# COMMAND ----------

display(predict_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) *Part 2:* Your Recommendations:
# MAGIC Let's look at what ALS recommended for your user based on your ratings

# COMMAND ----------

#TO-DO
#Filter the predictions DF for your user id something like "user_id = ID"
predictions = model.recommendForAllUsers(10)
my_predictions = predictions.filter(<<FILL-IN>>)

# COMMAND ----------

display(my_predictions)

# COMMAND ----------

from pyspark.sql.functions import *
my_recs = my_predictions.select("user_id", explode("recommendations").alias("recommendations")).select("user_id", "recommendations.product_id", "recommendations.rating").join(product_df, ['product_id'])