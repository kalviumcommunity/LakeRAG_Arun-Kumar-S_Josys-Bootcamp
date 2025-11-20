package config

object S3Config {
  val bucket: String = "lakerag-arun-bootcamp"

  // Input folder: where you drop PDF/TXT/JSON
  val rawInputPrefix: String = "raw/"

  // Output folder for raw delta tables
  val rawDeltaPrefix: String = "raw-delta/"

  val rawInputPath: String = s"s3a://$bucket/$rawInputPrefix"
  val rawDeltaPath: String = s"s3a://$bucket/$rawDeltaPrefix"
}
