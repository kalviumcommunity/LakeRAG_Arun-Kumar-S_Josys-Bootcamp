package etl.raw

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object RawWriter {

  def write(df: DataFrame, out: String): Unit = {

    val dqFail = df.filter(
      col("file_path").isNull ||
      col("content").isNull ||
      length(trim(col("content"))) < 1
    )

    // FIXED: compute count once → avoid isEmpty + double actions
    val failCount = dqFail.count()

    if (failCount > 0) {
      println(s"[RawWriter:DQ] ❌ Found $failCount bad rows, NOT writing them.")
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
