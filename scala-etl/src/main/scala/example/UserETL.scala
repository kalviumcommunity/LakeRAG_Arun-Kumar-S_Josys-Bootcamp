package example

import org.apache.spark.sql.{SparkSession, functions => F}
import io.delta.tables._

object UserETL {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("UserETL")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    println("\n=== Starting ETL Job (Delta Lake) ===\n")

    // 1. Read raw CSV
    val rawDf = spark.read
      .option("header", "true")
      .csv("data/raw/users.csv")

    println("Raw Data:")
    rawDf.show(false)

    // 2. Clean data (fill missing values)
    val cleanedDf = rawDf
      .withColumn("age", F.coalesce(F.col("age"), F.lit(0)))
      .withColumn("city", F.coalesce(F.col("city"), F.lit("Unknown")))

    println("Cleaned Data:")
    cleanedDf.show(false)

    // 3. Write to Silver (Delta Lake)
    cleanedDf.write
      .format("delta")
      .mode("overwrite")
      .save("data/silver/users")

    println("\n=== ETL Completed (Silver DELTA written) ===\n")

    spark.stop()
  }
}
