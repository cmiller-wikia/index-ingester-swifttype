package fandom.search.ingester

import cats.effect._
import fs2.{ Stream, Pipe }
import fs2.io._
import java.io.File
import scala.concurrent.ExecutionContext

object files {
  def spoolToTempFile(implicit blockingExecutionContext: ExecutionContext, cs: ContextShift[IO]): Pipe[IO, Byte, Byte] =
    in ⇒
      Stream.eval(writeToTempFile(in))
        .flatMap { f ⇒ file.readAll[IO](f.toPath, blockingExecutionContext, 64 * 1024).onFinalize(IO { f.delete(); () }) }

  def writeToTempFile(in: Stream[IO, Byte])(implicit blockingExecutionContext: ExecutionContext, cs: ContextShift[IO]): IO[File] =
    for {
      f ← IO { File.createTempFile("spool", ".tmp", new File(".")) }
      _ ← IO { f.deleteOnExit }
      _ ← in.to(file.writeAll(f.toPath, blockingExecutionContext)).compile.drain
    } yield (f)
}
