# Databricks notebook source
dbutils.secrets.list("formula1-scope")

# COMMAND ----------

storage_account_name = "formula1dl2022"
client_id = dbutils.secrets.get(scope = "formula1-scope", key = "databricks-app-client-id")
tenant_id = dbutils.secrets.get(scope = "formula1-scope", key = "databricks-app-tenant-id")
client_secret = dbutils.secrets.get(scope = "formula1-scope", key = "databricks-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
    try: 
        dbutils.fs.mount(
            source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
            mount_point = f"/mnt/{storage_account_name}/{container_name}",
            extra_configs = configs
        )
    except:
        print("Already mounted!")

# COMMAND ----------

mount_adls("raw")
mount_adls("raw2")
mount_adls("rawpickle")
mount_adls("processed")
mount_adls("presentation")
mount_adls("processeddelta")
mount_adls("presentationdelta")
mount_adls("demo")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

print(dbutils.fs.ls("/mnt/formula1dl2022/raw"))
print(dbutils.fs.ls("/mnt/formula1dl2022/raw2"))
print(dbutils.fs.ls("/mnt/formula1dl2022/processed"))
print(dbutils.fs.ls("/mnt/formula1dl2022/presentation"))
print(dbutils.fs.ls("/mnt/formula1dl2022/processeddelta"))
print(dbutils.fs.ls("/mnt/formula1dl2022/presentationdelta"))
print(dbutils.fs.ls("/mnt/formula1dl2022/demo"))

# COMMAND ----------

#dbutils.fs.unmount("/mnt/formula1dl2022/processed")
#dbutils.fs.unmount("/mnt/formula1dl2022/raw")
#dbutils.fs.unmount("/mnt/formula1dl2022/presentation)

# COMMAND ----------

#rimuove file dbfs
dbutils.fs.rm("/mnt/formula1dl", True)
