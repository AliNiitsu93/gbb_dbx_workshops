# Databricks notebook source
# MAGIC %md # Table ACLs Setup by an Admin

# COMMAND ----------

# MAGIC %md #### Stuff to be done to enable both table and row-level grants to a non-admin user

# COMMAND ----------

# MAGIC %sql SHOW GRANT `abhinavg6@gmail.com` ON DATABASE default

# COMMAND ----------

# MAGIC %sql GRANT SELECT ON TABLE default.country_codes to `abhinavg6@gmail.com`

# COMMAND ----------

# MAGIC %sql SHOW GRANT `abhinavg6@gmail.com` ON TABLE default.country_codes

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW default.states_ne AS SELECT * FROM default.states where state_abbr in ('MA','VT','ME','NH','RI','CT');
# MAGIC GRANT SELECT ON VIEW default.states_ne to `abhinavg6@gmail.com`;

# COMMAND ----------

# MAGIC %sql SHOW GRANT `abhinavg6@gmail.com` ON VIEW default.states_ne

# COMMAND ----------

# MAGIC %sql SHOW GRANT `abhinavg6@gmail.com` ON TABLE default.states

# COMMAND ----------

# MAGIC %md #### Some stuff to be done for underlying table, if it was not created with Table ACLs "on"
# MAGIC * See the [FAQ](https://docs.azuredatabricks.net/administration-guide/admin-settings/table-acls/object-permissions.html#frequently-asked-questions)

# COMMAND ----------

# MAGIC %sql ALTER TABLE default.states OWNER TO `abhinav.garg@databricks.com`

# COMMAND ----------

# MAGIC %sql SHOW GRANT `abhinav.garg@databricks.com` ON TABLE default.states

# COMMAND ----------

# MAGIC %sql SHOW GRANT `abhinav.garg@databricks.com` ON VIEW default.states_ne