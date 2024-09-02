# Databricks notebook source
import pyspark.sql.types as T
import pyspark.sql.functions as F

dbutils.widgets.text("accessKeyName", "")
dbutils.widgets.text("accessKeyValue", "")

# Event Hubs configuration
EH_NAMESPACE                    = "a9o-iotdemo-eh01"
EH_NAME                         = "sensordata01"

EH_CONN_SHARED_ACCESS_KEY_NAME  = dbutils.widgets.get("accessKeyName")
EH_CONN_SHARED_ACCESS_KEY_VALUE = dbutils.widgets.get("accessKeyValue")

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
payload_ddl = """battery_level BIGINT, c02_level BIGINT, cca2 STRING, cca3 STRING, cn STRING, device_id BIGINT, device_name STRING, humidity BIGINT, ip STRING, latitude DOUBLE, lcd STRING, longitude DOUBLE, scale STRING, temp  BIGINT, timestamp BIGINT"""
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


df = (spark.readStream
  .format("kafka")
  .options(**KAFKA_OPTIONS)
  .load()
  .transform(parse)
)

display(df)
