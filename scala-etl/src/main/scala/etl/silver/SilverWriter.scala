package etl.silver

import org.apache.spark.sql.{DataFrame, SaveMode}

object SilverWriter {

  def write(df: DataFrame, out: String): Unit = {
    df.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(out)
  }
}
