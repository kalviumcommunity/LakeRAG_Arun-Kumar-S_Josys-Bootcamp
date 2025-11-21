package etl.raw

import ingestion.{PdfExtractor, TextExtractor, JsonExtractor}
import org.apache.spark.sql.{DataFrame, SparkSession}

object RawReader {

  def read(spark: SparkSession, format: String, path: String): DataFrame = {

    println(s"[RawReader] Reading file ($format): $path")

    format match {
      case "txt"  => TextExtractor.extract(spark, path)
      case "json" => JsonExtractor.extract(spark, path)
      case "pdf"  => PdfExtractor.extract(spark, path)
      case other  =>
        throw new IllegalArgumentException(s"Unsupported format: $other")
    }
  }
}
