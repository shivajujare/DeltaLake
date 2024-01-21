# Databricks notebook source
storage_account_name = "staccount218"
client_id            = dbutils.secrets.get(scope="kv-secret-scope", key="sp-client-id")
tenant_id            = dbutils.secrets.get(scope="kv-secret-scope", key="sp-tenant-id")
client_secret        = dbutils.secrets.get(scope="kv-secret-scope", key="sp-secret-value")

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
           
def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

#mounting the container created to store the raw files 
mount_adls("output")

dbutils.fs.mounts()

#dbutils.fs.ls("/mnt/datasetbigdata/raw-data/lending_loan/")


# COMMAND ----------

