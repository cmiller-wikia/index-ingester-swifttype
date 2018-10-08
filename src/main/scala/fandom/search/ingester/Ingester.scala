package fandom.search.ingester

import cats.effect.IO
import fs2.{ Stream, Pipe, Sink }
import fs2.io._
import okhttp3._
import java.io.File
import scala.concurrent.ExecutionContext
import com.typesafe.scalalogging._
import io.circe.{ Json, JsonObject }
import io.circe.fs2.{ byteArrayParser ⇒ streamJson }
import Config._
import Predef.wrapString

object Ingester {
  val logger = Logger("IngestLog")
  implicit val ioThreadPool = ExecutionContext.fromExecutorService(java.util.concurrent.Executors.newCachedThreadPool())
  val maxConcurrentRequests = 10
  val fileNames: Seq[String] = Range(0, 26).map("verbose-%06d.json".format(_))

  def run: IO[Unit] =
    Stream.bracket(Clients.apply)(
      c ⇒ indexFromS3(
        c.http,
        new S3Downloader(S3Bucket(s3Bucket), c.s3),
        SwiftType(Config.st_engine, HttpUrl.parse(Config.st_api_uri), Config.st_private_key)
      ),
      c ⇒ c.shutdown
    ).compile.drain

  def indexFromS3(httpClient: OkHttpClient, s3: S3Downloader, swiftType: SwiftType): Stream[IO, Unit] = {
    Stream.emits(fileNames)
      .flatMap(s3.download)
      .through(spoolToTempFile)
      .through(streamJson)
      .through(skipIfNotJsonObject)
      .through(batchBy(100))
      .map(swiftType.index(httpClient))
      .join(maxConcurrentRequests)
      .to(logOnlyErrors)
  }

  def batchBy[F[_], A](maxBatchSize: Int): Pipe[F, A, List[A]] =
    _.segmentN(maxBatchSize, true).map(_.force.toList)

  def spoolToTempFile: Pipe[IO, Byte, Byte] =
    in ⇒
      Stream.eval(writeToTempFile(in))
        .flatMap { f ⇒ file.readAll[IO](f.toPath, 64 * 1024).onFinalize(IO { f.delete(); () }) }

  def writeToTempFile(in: Stream[IO, Byte]): IO[File] =
    for {
      f ← IO { File.createTempFile("aws", "tmp", new File(".")) }
      _ ← IO { f.deleteOnExit }
      _ ← in.to(file.writeAll(f.toPath)).compile.drain
    } yield (f)

  def skipIfNotJsonObject[F[_]]: Pipe[F, Json, JsonObject] =
    _.flatMap(_.asObject.map(Stream.emit(_)).getOrElse(Stream.empty))

  def logOnlyErrors: Sink[IO, IngestResult] = _.flatMap(
    _ match {
      case IngestSuccess(_) ⇒ Stream.empty
      case x ⇒ Stream.eval(IO { logger.error(x.toString) })
    }
  )
}
