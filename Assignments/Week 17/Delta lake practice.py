# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists testdb

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

cusdf = spark.read \
            .format("csv") \
            .option("header", True) \
            .option("path","/mnt/staccount218/input/custhead.csv") \
            .load()
cusdf.show(5)

# COMMAND ----------


cusdf.write \
    .option("header",True) \
    .format("parquet") \
    .mode("overwrite") \
    .option("path","/mnt/staccount218/output/parquet/") \
    .saveAsTable("testdb.custpq")

# COMMAND ----------

spark.sql("drop table if exists testdb.custhead")

cusdf.write \
    .option("header",True) \
    .format("delta") \
    .mode("overwrite") \
    .option("path","/mnt/staccount218/output/delta/") \
    .saveAsTable("testdb.custhead")

# COMMAND ----------

# MAGIC %sql
# MAGIC use database testdb;
# MAGIC
# MAGIC select * from custhead limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC use database testdb;
# MAGIC
# MAGIC select * from custpq limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history custhead

# COMMAND ----------

# MAGIC %sql
# MAGIC create table testdb.custhead2 using delta location "/mnt/staccount218/output/delta/"

# COMMAND ----------

# MAGIC %sql
# MAGIC show

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from custhead2 limit 4

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into testdb.custhead 
# MAGIC values (123123
# MAGIC ,'test'
# MAGIC ,'Hernandez'
# MAGIC ,'XXXXXXXXX'
# MAGIC ,'XXXXXXXXX'
# MAGIC ,'6303 Heather Plaza'
# MAGIC ,'Brownsville'
# MAGIC ,'TX'
# MAGIC ,'78521'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from testdb.custhead version as of 3 where cust_id = 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from testdb.custhead where cust_id =4;

# COMMAND ----------

# MAGIC %sql
# MAGIC update testdb.custhead set cust_fname = 'update_name' where cust_id = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history testdb.custhead

# COMMAND ----------

cusdf2 = spark.read \
            .format("csv") \
            .option("header", True) \
            .option("path","/mnt/staccount218/input/custheadcopy.csv") \
            .load()

cusdf2.show(5)
            

# COMMAND ----------

cusdf2.write \
        .format("delta") \
        .option("header", True) \
        .option("mergeSchema",True) \
        .option("path","/mnt/staccount218/output/delta/") \
        .mode("append") \
        .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from testdb.custhead where cust_id in (123322)

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table testdb.custhead to version as of 0

# COMMAND ----------

