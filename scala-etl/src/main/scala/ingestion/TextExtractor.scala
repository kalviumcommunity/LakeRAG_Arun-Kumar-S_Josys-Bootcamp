package ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.sql.Timestamp
import java.time.Instant

object TextExtractor {

  def extract(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    val ts = Timestamp.from(Instant.now())

    val files = spark.sparkContext.binaryFiles(path).map {
      case (filePath, content) =>
        val text = new String(content.toArray(), "UTF-8")
        (filePath, text, ts)
    }

    files.toDF("file_path", "content", "ingested_at")
  }
}
