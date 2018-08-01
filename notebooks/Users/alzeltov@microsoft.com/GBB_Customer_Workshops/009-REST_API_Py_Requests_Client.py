# Databricks notebook source
# MAGIC %md # Python REST Client using Wrapper Module

# COMMAND ----------

# MAGIC %md ####Load REST Class

# COMMAND ----------

# MAGIC %run ./008-REST_API_Py_Requests_Lib

# COMMAND ----------

# MAGIC %md ####Initialize REST Object

# COMMAND ----------

import datetime
import json

# initialize the DBC_Rest Object
rest = DBC_REST_API("https://eastus2.azuredatabricks.net", "replaceWithYourToken")

# COMMAND ----------

# MAGIC %md ####1) Need to get cluster ID of cluster we want job to run on
# MAGIC When running a job that creates its own cluster the cluster name will be of the form: job-[job_id]-run-[run_id]

# COMMAND ----------

cluster_name = 'Tritium'
# Get cluster ID based on cluster name
runnerClusterId = rest.get_clusterID(cluster_name, comp_type = 'eq', state = 'RUNNING')
print 'Runner Cluster Id:  %s' % runnerClusterId

# COMMAND ----------

# MAGIC %md #### 2) Get Job_ID of job you want to run on the cluster
# MAGIC Job ID can be found in the jobs tab next to the job name, alternatively we can look it up by name as well

# COMMAND ----------

# look up by name
job_name = 'test_dbutils_context'
job_id = rest.get_jobID(job_name)
if job_id < 0:
  raise ValueError("Error retrieving Job ID. Verify job name '{0}' exists.".format(job_name))

print("Job ID is {0}".format(job_id))

# COMMAND ----------

# MAGIC %md ####3) Update existing Job with Cluster ID
# MAGIC Now that we have the Cluster ID, the job to run on the cluster need to be updated with the correct cluster ID  
# MAGIC (this assumes job was initially configured to run on 'Existing cluster')

# COMMAND ----------

new_jobsettings = {"existing_cluster_id":runnerClusterId}
print new_jobsettings

# Reset Job's cluster id
rq1 = rest.reset_job(job_id, new_jobsettings)
if rq1.status_code != 200:
  raise ValueError('Trouble resetting job cluster id for '+job_name, rq1.status_code)

# COMMAND ----------

# MAGIC %md ####4) Run Job with parameters

# COMMAND ----------

source = "databricks-johndoe/test-input/"
test_target = "databricks-johndoe/test-output/"
run_mode =  'test'

params = {'source': source, 'target': test_target, 'run_mode': run_mode}

# run the job
req = rest.run_job(job_id, params)
#  Verify job started ok
if req.status_code == 200:
  # Run job returns run_id and number_in_job.  Save for polling and logging of data
  number_in_job=req.json()['number_in_job']
  print('Job Number: {0}'.format(number_in_job))
  run_id=req.json()['run_id']
  print('Run ID: {0}'.format(run_id))