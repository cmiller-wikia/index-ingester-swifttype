package fandom.search.ingester

import fs2.Stream
import fs2.io
import scala.util.Try
import cats.syntax.flatMap._
import cats.effect._
import java.io.File
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.transfer.model.UploadResult
import scala.concurrent._
import com.typesafe.scalalogging._

case class S3Bucket(value: String) extends AnyVal

class S3Uploader(bucket: S3Bucket, manager: TransferManager) {
  val logger = Logger("UploaderLog")
  def uploader(maxRetries: Int)(file: File): Stream[IO, Unit] = {
    def tryUpload(file: File, retry: Int): Stream[IO, Unit] = {
      def onError(t: Throwable): Stream[IO, Unit] =
        if (retry >= maxRetries)
          Stream.eval(IO { logger.error("Upload of " + file + " failed: " + t) })
        else
          Stream.eval(IO {
            logger.info("Upload of " + file + " failed (attempt: " + (retry + 1) + "): " + t)
          }) >> tryUpload(file, retry + 1)

      Stream.eval(upload(file)).flatMap {
        _.fold(
          onError,
          _ ⇒ Stream.eval(IO { file.delete(); () })
        )
      }
    }

    tryUpload(file, 0)
  }

  def upload(file: File): IO[Either[Throwable, UploadResult]] =
    for {
      _ ← IO { logger.info("Starting upload: " + file) }
      upload ← IO { manager.upload(bucket.value, file.getName(), file) }
      result ← IO { blocking { Try(upload.waitForUploadResult).toEither } }
      _ ← IO { logger.info("Ended upload: " + file) }
    } yield (result)
}

class S3Downloader(bucket: S3Bucket, client: AmazonS3) {
  val logger = Logger("DownloaderLog")
  def download(key: String): Stream[IO, Byte] =
    io.readInputStream[IO](
      IO { logger.info("Processing: " + key); client.getObject(bucket.value, key).getObjectContent },
      16 * 4096
    )
}
