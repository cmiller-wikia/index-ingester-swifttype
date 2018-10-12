package fandom.search.ingester

import cats.effect._
import cats.syntax.flatMap._
import fs2.{ Stream, Pipe, Sink, text }
import fs2.io._
import java.io.File
import java.nio.file.Path
import scala.concurrent.ExecutionContext
import com.typesafe.scalalogging._
import io.circe.Json
import io.circe.fs2.{ byteArrayParser ⇒ streamJson }
import Config._
import Predef.wrapString
import Predef.ArrowAssoc
import files._

object Jinni {
  implicit val ioThreadPool = ExecutionContext.fromExecutorService(java.util.concurrent.Executors.newCachedThreadPool())
  implicit val ctxShift = IO.contextShift(ioThreadPool)
  val fileNames: Seq[String] = Range(0, 26).map("verbose-%06d.json".format(_))

  val wikis: List[(Path, String)] =
    List(
      "harrypotter" -> "509",
      "fallout" -> "3035",
      "gameofthrones" -> "130814",
      "dragonball" -> "530",
      "attackontitan" -> "338386",
      "callofduty" -> "3125",
      "breakingbad" -> "18733",
      "esharrypotter" -> "1044"
    ).map { case (name, id) ⇒ (new File(name + ".json").toPath, id) }

  def run: IO[Unit] =
    IO(Clients.apply)
      .bracket {
        _ >>= filterWikis
      } {
        _ >>= { _.shutdown }
      }

  def extractFlaggedWikis[F[_]: Sync: ContextShift]: List[Sink[F, Json]] = wikis.map {
    case (path, id) ⇒ extractWiki(id, path)
  }

  def extractWiki[F[_]: Sync: ContextShift](wikiId: String, outFile: Path): Sink[F, Json] =
    _.through(filterWiki(wikiId))
      .map(_.noSpaces)
      .intersperse("\n")
      .through(text.utf8Encode)
      .to(file.writeAll(outFile, ioThreadPool))

  def filterWikis(c: Clients): IO[Unit] =
    filterFromS3(new S3Downloader(S3Bucket(s3Bucket), c.s3)) >> reuploadResults(new S3Uploader(S3Bucket(s3Bucket), c.s3Txfr))

  def reuploadResults(uploader: S3Uploader): IO[Unit] =
    Stream.emits(wikis.map(_._1).map(_.toFile))
      .map(uploader.uploader(5)(_))
      .parJoin(3)
      .compile
      .drain

  def filterWiki[F[_]](wikiId: String): Pipe[F, Json, Json] =
    _.filter(_.hcursor.get[String]("wiki_id").toOption == Some(wikiId))

  def filterFromS3(s3: S3Downloader): IO[Unit] = {
    Stream.emits(fileNames)
      .flatMap(
        s3.download(_)
          .through(spoolToTempFile)
          .through(streamJson)
      )
      .broadcastTo(extractFlaggedWikis[IO]: _*)
      .compile
      .drain
  }
}
