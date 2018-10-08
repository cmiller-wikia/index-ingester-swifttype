package fandom.search.ingester

import cats.effect.IO
import com.amazonaws.services.s3.transfer._
import com.amazonaws.services.s3.{ AmazonS3, AmazonS3ClientBuilder }
import okhttp3._
import com.typesafe.scalalogging._
import cats.syntax.flatMap._

case class Clients(s3: AmazonS3, s3Txfr: TransferManager, http: OkHttpClient) {
  def shutdown: IO[Unit] =
    Clients.shutdownS3Client(s3) >> Clients.shutdownS3Txfr(s3Txfr) >> Clients.shutdownHttpClient(http)
}

object Clients {
  val logger = Logger("Clients")

  def httpClient: IO[OkHttpClient] = IO {
    import java.util.concurrent.TimeUnit._

    new OkHttpClient.Builder()
      .connectTimeout(1, MINUTES)
      .readTimeout(1, MINUTES)
      .build()
  }

  def s3Client: IO[AmazonS3] = IO {
    AmazonS3ClientBuilder.defaultClient()
  }

  def s3Txfr: IO[TransferManager] = IO {
    TransferManagerBuilder.standard().build()
  }

  def shutdownS3Txfr(tx: TransferManager): IO[Unit] = IO {
    logger.info("Shutting down S3 txfr client")
    tx.shutdownNow
  }

  def shutdownS3Client(s3: AmazonS3): IO[Unit] = IO {
    logger.info("Shutting down S3 client")
    s3.shutdown
  }

  def shutdownHttpClient(client: OkHttpClient): IO[Unit] = IO.unit

  def apply: IO[Clients] = for {
    s3 ← s3Client
    txfr ← s3Txfr
    http ← httpClient
  } yield (Clients(s3, txfr, http))
}
