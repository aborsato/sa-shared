# Databricks notebook source
# MAGIC %pip install pyyaml
# MAGIC %restart_python

# COMMAND ----------

import os
import logging
import json
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
import yaml

# Create and set the level of logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class MetadataProcess:
  def __init__(self, catalog, schema, spec_name):
    self.catalog = catalog
    self.schema = schema
    self.config = self._load_spec(spec_name)
    self.table_schema = self._load_schema(spec_name)
  
  def _load_spec(self, spec_name):
    with open(f'config/{spec_name}.spec.yaml', 'r') as f:
      return yaml.safe_load(f)

  def _load_schema(self, spec_name):
    file_path = f'config/{spec_name}.schema.json'
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return StructType.fromJson(json.load(f))
    return None
  
  def read_source(self) -> DataFrame:
    def _file(file) -> DataFrame:
      logger.info(f"read_source.file({file['path']})")
      return (
        spark
        .read
        .format(file['format'])
        .load(file['path'])
      )

    source = self.config['source']
    if 'file' in source:
      return _file(source['file'])

  def create_transform_function(self, transform):
    def _dropDuplicates(df: DataFrame) -> DataFrame:
      logger.info(f"transform.dedupe.dropDuplicates({transform['columns']})")
      return df.dropDuplicates(transform['columns'])

    def _rank(df: DataFrame) -> DataFrame:
      logger.info(f"transform.dedupe.rank({transform['columns']})")
      window = (
        Window
        .partitionBy(transform['columns'])
        .orderBy([F.desc(c) for c in transform['order']])
      )
      return (
        df
        .withColumn('rank', F.rank().over(window))
        .filter("rank == 1")
        .drop('rank')
      )

    def _filter(df: DataFrame) -> DataFrame:
      logger.info(f"transform.filter({transform['condition']})")
      return df.filter(transform['condition'])

    def _select(df: DataFrame) -> DataFrame:
      columns = transform['columns'] if 'columns' in transform else ['*']
      drop = transform['drop'] if 'drop' in transform else []
      logger.info(f"transform.dedupe.select({columns} except {drop})")
      if drop:
        return df.select(*columns).drop(*drop)
      else:
        return df.select(*columns)

    if transform['type'] == 'dedupe':
      if transform['method'] == 'dropDuplicates':
        return _dropDuplicates
      elif transform['method'] == 'rank':
        return _rank
    elif transform['type'] == 'filter':
      return _filter
    elif transform['type'] == 'select':
      return _select

  def transform(self, df: DataFrame) -> DataFrame:
    transforms = self.config['transform']
    for transform in transforms:
      df = df.transform(self.create_transform_function(transform))
    return df

  def write_target(self, df: DataFrame, posfix):
    target = self.config['target']
    table_name = f"{self.config['name']}{('_' + posfix if posfix else '')}"
    logger.info(f'write_target({table_name})')
    return (
      df
      .write
      .format('delta')
      .mode(target['mode'])
      .saveAsTable(f'{self.catalog}.{self.schema}.{table_name}')
    )

  def process(self):
    logger.info(f'write_target({table_name})')
    df = self.read_source()
    df = self.transform(df)
    if self.table_schema:
      df = df.to(self.table_schema)
    return self.write_target(df, 'bronze')


# COMMAND ----------

def run(spec):
  m = MetadataProcess('shared', 'a9o', spec)
  result = m.process()

run('customer')


# COMMAND ----------

# Read the table
# df = spark.sql("SELECT * FROM (SELECT * FROM tpcds.sf_1000.catalog_sales LIMIT 100000000) s JOIN tpcds.sf_1000.item i ON s.cs_item_sk = i.i_item_sk")

# Write the DataFrame as Parquet files to a specified volume
# output_path = "/Volumes/shared/a9o/raw-files/sales"
# df.write.mode("overwrite").parquet(output_path)
