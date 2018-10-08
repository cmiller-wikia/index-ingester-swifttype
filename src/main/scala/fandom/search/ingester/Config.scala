package fandom.search.ingester

object Config {
  lazy val s3Bucket: String = requiredEnv("S3_BUCKET")
  lazy val st_api_uri: String = requiredEnv("SWIFTTYPE_API_URL")
  lazy val st_private_key: String = requiredEnv("SWIFTTYPE_PKEY")
  lazy val st_engine: String = requiredEnv("SWIFTTYPE_ENGINE")

  def requiredEnv(key: String): String =
    sys.env.get(key).getOrElse(throw new RuntimeException("ENV: " + key + " is not set"))
}
