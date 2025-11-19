package example

import org.apache.spark.sql.{SparkSession, functions => F}
import io.delta.tables._

object UserAggregationETL {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("UserAggregationETL")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    println("\n=== Starting Silver -> Gold ETL (Delta Lake) ===\n")

    // 1. Read silver data (from Delta)
    val silverDf = spark.read
      .format("delta")
      .load("data/silver/users")

    println("Silver Data:")
    silverDf.show(false)

    // 2. Aggregation: count users by city
    val cityCountDf = silverDf
      .groupBy("city")
      .agg(F.count("*").alias("user_count"))

    println("City-wise User Count:")
    cityCountDf.show(false)

    // 3. Write to gold zone (Delta)
    cityCountDf.write
      .format("delta")
      .mode("overwrite")
      .save("data/gold/user_city_stats")

    println("\n=== Gold Dataset Written (Delta) → data/gold/user_city_stats ===\n")

    spark.stop()
  }
}
