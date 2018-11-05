package kkbox

import org.apache.spark.sql.functions.{col, desc, split, window}
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.streaming.Trigger.ProcessingTime


object structuredStreaming extends App {

  val spark = SparkSession.builder.appName(getClass.getName).getOrCreate()

  val kafkaDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.142.130:9092,192.168.142.128:9092")
    .option("subscribe", "kkboxINPUT")
    .option("startingOffsets", "latest")
    .load()

  val kkboxDF = kafkaDF.selectExpr("CAST(value AS STRING) as value")
    .withColumn("tmp", split(col("value"),","))
    .select(
      col("tmp").getItem(0).alias("msno"),
      col("tmp").getItem(1).alias("songID"),
      col("tmp").getItem(2).alias("sourceSystemTab"),
      col("tmp").getItem(3).alias("sourceScreenName"),
      col("tmp").getItem(4).alias("sourceType"),
      col("tmp").getItem(5).cast("timestamp").alias("time")
    )

  val topSongs = kkboxDF.select(col("time"),col("songID")).groupBy(
    window(col("time"),"6 minutes", "5 seconds"),
    col("songID")
  ).count().orderBy(desc("window"),desc("count"))

  topSongs
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()
    .awaitTermination()

}