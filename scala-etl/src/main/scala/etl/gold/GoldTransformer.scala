package etl.gold

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

object GoldTransformer {

  def chunkText(text: String, chunkSize: Int = 500): Seq[String] = {
    if (text == null || text.trim.isEmpty) Seq.empty
    else text.grouped(chunkSize).toSeq
  }

  def transform(silver: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // -------------------------
    // DQ CHECK (minimal, safe)
    // -------------------------
    val dq = silver
      .filter(col("doc_id").isNotNull && length(trim(col("doc_id"))) > 0)
      .filter(col("file_path").isNotNull && length(trim(col("file_path"))) > 0)
      .filter(col("content").isNotNull)

    dq.flatMap { row =>
      val docId        = row.getAs[String]("doc_id")
      val filePath     = row.getAs[String]("file_path")
      val content      = row.getAs[String]("content")
      val contentHash  = row.getAs[String]("content_hash")
      val ingestedAt   = row.getAs[Timestamp]("ingested_at")
      val goldTs       = Timestamp.from(Instant.now())

      val chunks = chunkText(content)

      chunks.zipWithIndex.map { case (chunk, idx) =>
        (
          UUID.randomUUID().toString,
          docId,
          filePath,
          chunk,
          idx,
          contentHash,
          ingestedAt,
          goldTs
        )
      }
    }.toDF(
      "chunk_id",
      "doc_id",
      "file_path",
      "chunk_text",
      "chunk_index",
      "content_hash",
      "ingested_at",
      "gold_timestamp"
    )
  }
}
