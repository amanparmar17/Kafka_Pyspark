from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_TOPIC_NAME = "sampleTopic"
KAFKA_SINK_TOPIC = "sinkTopic"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
# CHECKPOINT_LOCATION = "LOCAL DIRECTORY LOCATION (FOR DEBUGGING PURPOSES)"
CHECKPOINT_LOCATION = "/Users/aman.parmar/Documents/DATA_SCIENCE/KAFKA/CHECKPOINT"


if __name__ == "__main__":

    # STEP 1 : creating spark session object

    spark = (
        SparkSession.builder.appName("Kafka Pyspark Streamin Learning")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # STEP 2 : reading a data stream from a kafka topic

    sampleDataframe = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    base_df = sampleDataframe.selectExpr("CAST(value as STRING)", "timestamp")
    base_df.printSchema()

    # STEP 3 : Applying suitable schema

    sample_schema = (
        StructType()
        .add("col_a", StringType())
        .add("col_b", StringType())
        .add("col_c", StringType())
        .add("col_d", StringType())
    )

    info_dataframe = base_df.select(
        from_json(col("value"), sample_schema).alias("info"), "timestamp"
    )

    info_dataframe.printSchema()
    info_df_fin = info_dataframe.select("info.*", "timestamp")
    info_df_fin.printSchema()

    # STEP 4 : Creating query using structured streaming

    query = info_df_fin.groupBy("col_a").agg(
        approx_count_distinct("col_b").alias("col_b_alias"),
        count(col("col_c")).alias("col_c_alias"),
    )

    # query = query.withColumn("query", lit("QUERY3"))
    result_1 = query.selectExpr(
        "CAST(col_a AS STRING)",
        "CAST(col_b_alias AS STRING)",
        "CAST(col_c_alias AS STRING)",
    ).withColumn("value", to_json(struct("*")).cast("string"),)

    result = (
        result_1.select("value")
        .writeStream.trigger(processingTime="10 seconds")
        .outputMode("complete")
        .format("kafka")
        .option("topic", KAFKA_SINK_TOPIC)
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
        .awaitTermination()
    )

    # result.awaitTermination()

