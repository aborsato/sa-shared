# Databricks notebook source
# MAGIC %md
# MAGIC # Reading Notebooks files programatically
# MAGIC This notebook demonstrates how to list and read notebook files using Databricks SDK.
# MAGIC References: https://databricks-sdk-py.readthedocs.io/en/latest/workspace/workspace/workspace.html, https://docs.databricks.com/en/files/index.html

# COMMAND ----------

!pip install databricks.sdk --upgrade --quiet
dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType

dir_path = "/Users/alan.borsato@databricks.com/dev/sa-shared/"

w = WorkspaceClient()
notebooks = [
  c for c in w.workspace.list(dir_path, recursive=True)
  if c.object_type == ObjectType.NOTEBOOK
]
for n in notebooks:
  with w.workspace.download(n.path) as f:
    content = f.read()
    print(f"---- {n.path} ----")
    print(content)

