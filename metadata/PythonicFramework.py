# Databricks notebook source
import logging
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

# Create and set the level of logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def read_file(path, format):
  logger.info(f"read_source.file({path})")
  return lambda: (
    spark
    .read
    .format(format)
    .load(path)
  )

def transform_dropDuplicates(columns):
  logger.info(f"transform.dropDuplicates({columns})")
  return lambda df: df.dropDuplicates(columns)

def transform_rank(columns, order = None):
  logger.info(f"transform.rank({columns})")
  window = Window.partitionBy(columns)
  if order:
    window = window.orderBy([F.desc(c) for c in order])
  return lambda df: (
    df
    .withColumn('rank', F.rank().over(window))
    .filter("rank == 1")
    .drop('rank')
  )

def transform_filter(condition):
  logger.info(f"transform.filter({condition})")
  return lambda df: df.filter(condition)

def transform_select(columns = ['*'], drop = None):
  logger.info(f"transform.dedupe.select({columns} except {drop})")
  if drop:
    return lambda df: df.select(*columns).drop(*drop)
  else:
    return lambda df: df.select(*columns)

def write_table(mode, table_name):
  logger.info(f'write_target.table({table_name})')
  return lambda df, catalog, schema: (
    df
    .write
    .format('delta')
    .mode(mode)
    .saveAsTable(f'{catalog}.{schema}.{table_name}')
  )


# COMMAND ----------

spec1 = {
  'read_source': read_file('/Volumes/shared/a9o/raw-files/sales/*', 'parquet'),
  'transform': [
    transform_rank(['i_item_id', 'cs_sold_date_sk'], ['cs_sold_date_sk', 'cs_sold_time_sk']),
    transform_select(drop=['cs_item_sk', 'i_rec_start_date', 'i_item_desc', 'i_formulation']),
    transform_filter('cs_quantity > 50')
  ],
  'write_target': write_table('overwrite', 'sales')
}

# COMMAND ----------

def process(spec, catalog, schema):
  def apply_transform(df: DataFrame, transforms) -> DataFrame:
    for transform in transforms:
      df = df.transform(transform)
    return df

  df = spec['read_source']()
  df = apply_transform(df, spec['transform'])
  return spec['write_target'](df, catalog, schema)


# COMMAND ----------

process(spec1, 'shared', 'a9o')
