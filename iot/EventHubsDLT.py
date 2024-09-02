# Databricks notebook source
import dlt
import pyspark.sql.types as T
import pyspark.sql.functions as F

# Event Hubs configuration
EH_NAMESPACE                    = spark.conf.get("iot.ingestion.eh.namespace")
EH_NAME                         = spark.conf.get("iot.ingestion.eh.name")

EH_CONN_SHARED_ACCESS_KEY_NAME  = spark.conf.get("iot.ingestion.eh.accessKeyName")
EH_CONN_SHARED_ACCESS_KEY_VALUE = spark.conf.get("iot.ingestion.eh.accessKeyValue")

EH_CONN_STR                     = f"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={EH_CONN_SHARED_ACCESS_KEY_NAME};SharedAccessKey={EH_CONN_SHARED_ACCESS_KEY_VALUE}"
# Kafka Consumer configuration

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : "60000",
  "kafka.session.timeout.ms" : "30000",
  "failOnDataLoss"           : "false",
  "startingOffsets"          : "earliest" #latest
}

# PAYLOAD SCHEMA
payload_ddl = """Country STRING,
City STRING,
Date STRING,
Temperature BIGINT,
Humidity BIGINT,
`Wind Speed` BIGINT,
`Wind Direction` STRING,
Precipitation BIGINT,
`Cloud Cover` BIGINT,
Visibility BIGINT,
Pressure BIGINT,
`Dew Point` BIGINT,
`UV Index` BIGINT,
Sunrise STRING,
Sunset STRING,
Moonrise STRING,
Moonset STRING,
`Moon Phase` STRING,
Conditions STRING,
Icon STRING"""
payload_schema = T._parse_datatype_string(payload_ddl)

# Basic record parsing and adding ETL audit columns
def parse(df):
  return (df
    .withColumn("records", F.col("value").cast("string"))
    .withColumn("parsed_records", F.from_json(F.col("records"), payload_schema))
    .withColumn("iot_event_timestamp", F.expr("cast(from_unixtime(parsed_records.timestamp / 1000) as timestamp)"))
    .withColumn("eh_enqueued_timestamp", F.expr("timestamp"))
    .withColumn("eh_enqueued_date", F.expr("to_date(timestamp)"))
    .withColumn("etl_processed_timestamp", F.col("current_timestamp"))
    .withColumn("etl_rec_uuid", F.expr("uuid()"))
    .drop("records", "value", "key")
  )

@dlt.create_table(
  comment="Raw IOT Events",
  table_properties={
    "quality": "bronze",
    "pipelines.reset.allowed": "false" # preserves the data in the delta table if you do full refresh
  },
  partition_cols=["eh_enqueued_date"]
)
@dlt.expect("valid_topic", "topic IS NOT NULL")
@dlt.expect("valid records", "parsed_records IS NOT NULL")
def iot_raw():
  return (
   spark.readStream
    .format("kafka")
    .options(**KAFKA_OPTIONS)
    .load()
    .transform(parse)
  )
