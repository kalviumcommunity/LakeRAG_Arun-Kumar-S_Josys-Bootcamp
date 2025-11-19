package example

import org.apache.spark.sql.SparkSession

object HelloSpark {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("HelloSpark")
      .master("local[*]")
      .getOrCreate()

    println("\n=== Spark Started Successfully ===\n")

    val data = Seq(
      ("Arun", 1),
      ("Josys", 2),
      ("Bootcamp", 3)
    )

    val df = spark.createDataFrame(data).toDF("name", "id")
    df.show(false)

    spark.stop()
  }
}
