package ingestion

import org.apache.spark.sql.{SparkSession, DataFrame}

object TextExtractor {

  def extract(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    val rdd = spark.sparkContext.wholeTextFiles(path)
    rdd.toDF("file_path", "content")
  }
}
