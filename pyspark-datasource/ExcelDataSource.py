# Databricks notebook source
# MAGIC %md
# MAGIC # Using PySpark DataSource API
# MAGIC This notebook demonstrate how to use the new PySpark Data Source API to create an Excel reader.
# MAGIC
# MAGIC Reference: https://docs.databricks.com/en/pyspark/datasources.html

# COMMAND ----------

# MAGIC %pip install openpyxl --quiet --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType


class ExcelDataSource(DataSource):
    """
    An example data source for Excel files.
    """

    @classmethod
    def name(cls):
        return "excel"

    def schema(self):
        return "name string"

    def reader(self, schema: StructType):
        return ExcelDataSourceReader(schema, self.options)


class ExcelDataSourceReader(DataSourceReader):

    def __init__(self, schema, options):
        self.schema: StructType = schema
        self.options = options

    def read(self, partition):
        # Library imports must be within the read method
        import pandas as pd

        pdf = pd.read_excel(self.options.get("path", "nofile.xslx"))
        # I have to rename all columns with space and special chars
        renames = {
            c: c.replace(' ', '').replace('%', '')
            for c in pdf.columns
        }
        pdf = pdf.rename(columns=renames)
        for i, row in pdf.iterrows():
            yield tuple(row)

# COMMAND ----------

spark.dataSource.register(ExcelDataSource)

file_path = '/Volumes/shared/a9o/raw-files/EmployeeSampleData.xlsx'
xslx_schema = "EEID STRING, FullName STRING, JobTitle STRING, Department STRING, BusinessUnit STRING, Gender STRING, Ethnicity STRING, Age INTEGER, HireDate STRING, AnnualSalary INTEGER, Bonus FLOAT, Country STRING, City STRING, ExitDate STRING"


df = spark.read.format("excel").schema(xslx_schema).option("path", file_path).load()
display(df)

# COMMAND ----------

df.write.saveAsTable('shared.a9o.xls_employee')
