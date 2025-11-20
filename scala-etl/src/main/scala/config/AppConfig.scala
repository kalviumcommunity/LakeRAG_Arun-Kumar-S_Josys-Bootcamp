package config

object AppConfig {

  // Toggle here:
  // true  = write to S3
  // false = write to local filesystem
  val useS3: Boolean = false   // Set true when ready for S3 tests

  // Local raw delta path (for fast debugging)
  val localRawDeltaPath: String = "data/raw-delta/"

}
