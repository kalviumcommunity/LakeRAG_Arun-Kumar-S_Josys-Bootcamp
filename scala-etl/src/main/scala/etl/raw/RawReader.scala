package etl.raw

import org.apache.spark.sql.{SparkSession, DataFrame}
import ingestion.{TextExtractor, JsonExtractor, PdfExtractor}

object RawReader {

  def read(spark: SparkSession, fileType: String, path: String): DataFrame = {
    fileType match {
      case "txt"  => TextExtractor.extract(spark, path)
      case "json" => JsonExtractor.extract(spark, path)
      case "pdf"  => PdfExtractor.extract(spark, path)
      case _ => throw new Exception("Invalid type")
    }
  }
}
