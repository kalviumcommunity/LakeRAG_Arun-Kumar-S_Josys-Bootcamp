package jobs

import core.SparkBuilder
import config.AppConfig
import etl.silver.{SilverTransformer, SilverWriter}
import org.apache.spark.sql.SparkSession

object RawToSilverJob {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkBuilder.get("RawToSilver")

    val rawPath    = AppConfig.RAW_INGESTED
    val silverPath = AppConfig.SILVER

    println(s"📥 Reading RAW_INGESTED Delta: $rawPath")
    println(s"📤 Writing SILVER Delta: $silverPath")

    // Load raw_ingested delta
    val rawDf = spark.read.format("delta").load(rawPath)

    println(s"RAW_INGESTED Schema:")
    rawDf.printSchema()

    // Apply transformation → cleaning + metadata
    val silverDf = SilverTransformer.transform(rawDf)

    println("SILVER Preview:")
    silverDf.show(20, truncate = false)

    // Write to Silver Delta zone
    SilverWriter.write(silverDf, silverPath)

    println(s"✅ Silver layer generated successfully → $silverPath")
    spark.stop()
  }
}
