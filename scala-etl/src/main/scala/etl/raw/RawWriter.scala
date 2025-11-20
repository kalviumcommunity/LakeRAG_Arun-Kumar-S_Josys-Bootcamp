package etl.raw

import org.apache.spark.sql.{DataFrame, SaveMode}

object RawWriter {

  def write(df: DataFrame, out: String): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(out)
  }
}
