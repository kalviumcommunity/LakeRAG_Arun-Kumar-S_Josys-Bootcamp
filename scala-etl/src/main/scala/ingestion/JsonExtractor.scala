package ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.sql.Timestamp
import java.time.Instant

object JsonExtractor {

  def extract(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    val ts = Timestamp.from(Instant.now())

    // Read JSON as plain text
    val files = spark.sparkContext.binaryFiles(path).map {
      case (filePath, content) =>
        val rawJson = new String(content.toArray(), "UTF-8")
        (filePath, rawJson, ts)
    }

    files.toDF("file_path", "content", "ingested_at")
  }
}
