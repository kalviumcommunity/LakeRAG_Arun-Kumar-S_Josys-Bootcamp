package etl.raw

import ingestion.{PdfExtractor, TextExtractor, JsonExtractor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object RawReader {

  def read(spark: SparkSession, format: String, path: String): DataFrame = {

    println(s"[RawReader] Reading file ($format): $path")

    val df = format match {
      case "txt"  => TextExtractor.extract(spark, path)
      case "json" => JsonExtractor.extract(spark, path)
      case "pdf"  => PdfExtractor.extract(spark, path)
      case other  =>
        throw new IllegalArgumentException(s"Unsupported format: $other")
    }

    // 🟦 DQ CHECKS (MINIMAL, LOCAL, SAFE)

    val dqFailed = df.filter(
      col("file_path").isNull ||
      col("content").isNull ||
      length(trim(col("content"))) < 1
    )

    if (!dqFailed.isEmpty) {
      println(s"[RawReader:DQ] ❌ Found ${dqFailed.count()} bad rows in $path")
      dqFailed.show(false)
    }

    val cleaned = df.filter(
      col("file_path").isNotNull &&
      col("content").isNotNull &&
      length(trim(col("content"))) > 0
    )


    cleaned
  }
}
