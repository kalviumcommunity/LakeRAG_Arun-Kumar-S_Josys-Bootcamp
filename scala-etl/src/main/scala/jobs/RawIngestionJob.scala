package jobs

import core.SparkBuilder
import etl.raw.{RawReader, RawWriter}
import org.apache.spark.sql.functions._

object RawIngestionJob {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: RawIngestionJob <txt|json|pdf> <path>")
      sys.exit(1)
    }

    val fileType = args(0)
    val inputPath = args(1)
    val output = "data/raw_ingested/"

    val spark = SparkBuilder.get("RawIngestion")

    val df = RawReader.read(spark, fileType, inputPath)
      .withColumn("ingested_at", current_timestamp())

    df.show(false)

    RawWriter.write(df, output)

    println(s"Raw ingestion completed → $output")
    spark.stop()
  }
}
