# Databricks notebook source
configs = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class":spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add  to the source URI of your mount point.
dbutils.fs.mount(
    source = "abfss://bronze@azureworkflowstorage.dfs.core.windows.net/",
    mount_point = "/mnt/bronze",
    extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")}


dbutils.fs.mount(
    source = "abfss://silver@azureworkflowstorage.dfs.core.windows.net/",
    mount_point = "/mnt/silver",
    extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/silver")

# COMMAND ----------

dbutils.fs.unmount("/mnt/gold")

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")}


dbutils.fs.mount(
    source = "abfss://gold@azureworkflowstorage.dfs.core.windows.net/",
    mount_point = "/mnt/gold",
    extra_configs = configs)

# COMMAND ----------

