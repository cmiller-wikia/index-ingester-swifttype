package fandom.search.ingester

import cats.effect.IO
import fs2.{ Stream, Pipe, Sink }
import okhttp3._
import scala.concurrent.ExecutionContext
import com.typesafe.scalalogging._
import io.circe.{ Json, JsonObject }
import io.circe.fs2.{ byteArrayParser ⇒ streamJson }
import Config._
import Predef.wrapString
import files._

object Ingester {
  val logger = Logger("IngestLog")
  implicit val ioThreadPool = ExecutionContext.fromExecutorService(java.util.concurrent.Executors.newCachedThreadPool())
  implicit val ctxShift = IO.contextShift(ioThreadPool)
  val maxConcurrentRequests = 10
  val fileNames: Seq[String] = Range(0, 26).map("verbose-%06d.json".format(_))

  def run: IO[Unit] =
    Stream.bracket(Clients.apply)(c ⇒ c.shutdown).flatMap(
      c ⇒ indexFromS3(
        c.http,
        new S3Downloader(S3Bucket(s3Bucket), c.s3),
        SwiftType(Config.st_engine, HttpUrl.parse(Config.st_api_uri), Config.st_private_key)
      )
    ).compile.drain

  def indexFromS3(httpClient: OkHttpClient, s3: S3Downloader, swiftType: SwiftType): Stream[IO, Unit] = {
    Stream.emits(fileNames)
      .flatMap(s3.download)
      .through(spoolToTempFile)
      .through(streamJson)
      .through(skipIfNotJsonObject)
      .through(util.batchBy(100))
      .map(swiftType.index(httpClient))
      .parJoin(maxConcurrentRequests)
      .to(logOnlyErrors)
  }

  def skipIfNotJsonObject[F[_]]: Pipe[F, Json, JsonObject] =
    _.flatMap(_.asObject.map(Stream.emit(_)).getOrElse(Stream.empty))

  def logOnlyErrors: Sink[IO, IngestResult] = _.flatMap(
    _ match {
      case IngestSuccess(_) ⇒ Stream.empty
      case x ⇒ Stream.eval(IO { logger.error(x.toString) })
    }
  )
}
