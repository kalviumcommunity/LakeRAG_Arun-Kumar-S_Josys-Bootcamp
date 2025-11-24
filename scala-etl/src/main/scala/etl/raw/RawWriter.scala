// RawWriter.scala
package etl.raw

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object RawWriter {

  def write(df: DataFrame, out: String): Unit = {

    // 🟦 Minimal DQ CHECK (safe, local)
    val dqFail = df.filter(
      col("file_path").isNull ||
      col("content").isNull ||
      length(trim(col("content"))) < 1
    )

    if (!dqFail.isEmpty) {
      println(s"[RawWriter:DQ] ❌ Found ${dqFail.count()} bad rows, NOT writing them.")
      dqFail.show(false)
    }

    val cleaned = df.filter(
      col("file_path").isNotNull &&
      col("content").isNotNull &&
      length(trim(col("content"))) > 0
    )

    cleaned.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(out)
  }
}
