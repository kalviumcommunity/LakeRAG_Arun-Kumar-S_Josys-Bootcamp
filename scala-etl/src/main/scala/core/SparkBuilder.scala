package core

import org.apache.spark.sql.SparkSession

object SparkBuilder {

  def get(appName: String): SparkSession = {

    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      // S3 config (won't break local)
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()
  }
}
